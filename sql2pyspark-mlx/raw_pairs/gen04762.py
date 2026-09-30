from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy("amount", "id")
w_ordered = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("is_credit").desc(), "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w5 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-06-12 12:00:00").cast("timestamp")) & (~F.col("merchant").eqNullSafe(F.lit("Amazon"))))
    .select(
        "*",
        F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    latest
    .withColumn("amount_avg_6", F.avg("amount").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w3))
)

cleaned = (
    metrics
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-06-21"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        "is_credit",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        (F.col("amount_avg_6") - F.col("is_credit")).alias("amount_avg_6_minus_is_credit"),
    )
)

gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w4).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w4) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged_flag",
        "amount_avg_6_minus_is_credit",
        F.sum("is_new_session").over(w5).alias("session_id"),
    )
)

result = (
    sessionized
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .cube(F.col("a.account_type").alias("account_type"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.is_flagged_flag")).alias("max_is_flagged_flag"),
        F.grouping_id().alias("level_id"),
    )
)
