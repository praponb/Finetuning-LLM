from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("burst_id").desc(), "id")

filtered = (
    df_transactions
    .filter(F.col("account_id").isin([6, 20]))
    .select("*", F.col("is_flagged").cast("int").alias("is_flagged_flag"))
)

cleaned = (
    filtered
    .filter(F.col("merchant").isNotNull() & (F.col("merchant") == "Grab"))
    .select(
        "*",
        F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0).alias("is_7_eleven"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.opened_date"), F.col("b.status"))
)

with_gaps = (
    enriched
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_flagged_flag",
        "is_7_eleven",
        "merchant_clean",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

base = (
    islands
    .filter((F.col("txn_time") >= F.lit("2023-06-02 10:00:00").cast("timestamp")) & F.col("is_flagged_flag").between(24, 40))
    .withColumn("days_since_txn_time", F.datediff(F.current_date(), "txn_time"))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "txn_type",
        "txn_time",
        "is_7_eleven",
        "merchant_clean",
        "burst_id",
        "days_since_txn_time",
    )
)

result = (
    latest
    .groupBy(F.date_trunc("quarter", "txn_time").alias("quarter_start"), "burst_id")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.percentile_approx("is_7_eleven", [0.25, 0.5, 0.75]).alias("is_7_eleven_quartiles"),
        F.avg(F.when(F.col("txn_type") == "DEBIT", F.col("is_7_eleven"))).alias("debit_avg_is_7_eleven"),
        F.max("txn_time").alias("ended_at"),
        F.max("txn_time").alias("ended_at_2"),
        F.round(F.avg("is_7_eleven"), 2).alias("avg_is_7_eleven"),
    )
    .orderBy(F.col("debit_avg_is_7_eleven").desc())
)
