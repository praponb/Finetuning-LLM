from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")
w2 = Window.orderBy(F.col("amount").desc(), "id")
w_running = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("running_avg_next_amount", "id")

enriched = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.lag("amount").over(w).alias("prev_amount"),
        F.lead("amount").over(w).alias("next_amount"),
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w2).alias("amount_bucket"),
    )
)

scored = (
    enriched
    .filter(F.col("next_amount") < 1000)
    .select(
        "id",
        "txn_time",
        "merchant",
        "next_amount",
        "row_seq",
        "amount_bucket",
        F.sum("next_amount").over(w_running).alias("running_sum_next_amount"),
        F.max("next_amount").over(w_running).alias("running_max_next_amount"),
        F.avg("next_amount").over(w_running).alias("running_avg_next_amount"),
        F.min("next_amount").over(w_running).alias("running_min_next_amount"),
        F.last("next_amount", ignorenulls=True).over(w4).alias("last_known_next_amount"),
    )
)

valid = (
    scored
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-04-16 12:00:00")))
    .select(
        "id",
        "merchant",
        "next_amount",
        "row_seq",
        "running_avg_next_amount",
        "running_min_next_amount",
        F.round((F.col("running_max_next_amount") / F.col("running_min_next_amount")), 2).alias("running_max_next_amount_per_running_min_next_amount"),
        F.round((F.col("running_sum_next_amount") / F.col("last_known_next_amount")), 2).alias("running_sum_next_amount_per_last_known_next_amount"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .withColumn("running_sum_next_amount_per_last_known_next_amount_tier", F.when(F.col("running_sum_next_amount_per_last_known_next_amount") >= 79, "HIGH").otherwise("LOW"))
    .cube("running_sum_next_amount_per_last_known_next_amount_tier", "merchant")
    .agg(
        F.round(F.avg("next_amount"), 2).alias("avg_next_amount"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("running_min_next_amount")).otherwise(0)).alias("amazon_running_min_next_amount"),
        F.grouping("running_sum_next_amount_per_last_known_next_amount_tier").alias("gid"),
    )
    .filter(F.col("amazon_running_min_next_amount") < 1000000)
    .orderBy(F.col("merchant").asc_nulls_first())
)
