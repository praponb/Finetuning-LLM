from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("merchant")
w3 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("account_id", "merchant").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w6 = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount_share_2").desc(), "id")
w7 = Window.partitionBy("merchant", "account_id")
w8 = Window.partitionBy("account_id")
w9 = Window.partitionBy("merchant").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-04-21").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.date_trunc("month", "txn_time").alias("txn_time_period"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share_2"),
        F.ntile(5).over(w3).alias("amount_bucket"),
        F.sum("amount").over(w4).alias("cumulative_amount"),
        F.ntile(3).over(w_recent).alias("amount_bucket_2"),
    )
)

enriched = (
    scored
    .withColumn("amount_bucket_share", F.round((F.col("amount_bucket") / F.sum("amount_bucket").over(w)), 4))
    .withColumn("amount_share_2_bucket", F.ntile(3).over(w6))
    .withColumn("merchant_count_cumulative_amount", F.count("cumulative_amount").over(w7))
    .withColumn("cumulative_amount_share", F.round((F.col("cumulative_amount") / F.sum("cumulative_amount").over(w8)), 4))
    .withColumn("running_amount_bucket", F.sum("amount_bucket").over(w9))
)

result = (
    enriched
    .groupBy("merchant")
    .agg(
        F.min("txn_time_period").alias("started_at"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.percentile_approx("cumulative_amount_share", 0.5).alias("cumulative_amount_share_p50"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("merchant").desc_nulls_last())
)
