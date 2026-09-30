from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-09-15 22:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.trim("merchant").alias("clean_merchant"),
    )
)

joined = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.customer_id") == 28)
    .select("s.*", F.col("a.balance"), F.col("a.opened_date"))
)

valid = (
    joined
    .filter((F.col("txn_time") > F.to_timestamp(F.lit("2023-07-28 22:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "clean_merchant",
        "balance",
        "opened_date",
        F.when(F.col("txn_type").isin(["CREDIT", "DEBIT"]), "Group A").otherwise("Other").alias("txn_type_group"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

cleaned_transactions = (
    valid
    .filter(F.col("balance") >= 37000)
    .withColumn("opened_date_month", F.month("opened_date"))
    .withColumn("txn_time_period", F.date_trunc("WEEK", "txn_time"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("OTHER")))
)

flagged = (
    cleaned_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("QUARTER", "txn_time_period").alias("quarter_start"),
        F.when(F.col("opened_date_month") >= 78, "PREMIUM").otherwise("STANDARD").alias("opened_date_month_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
)
