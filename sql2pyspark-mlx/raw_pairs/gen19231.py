from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_amount", "id")
w4 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.orderBy("running_amount_bucket_2")
w6 = Window.orderBy("running_amount_bucket", "id")

enriched = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "is_flagged",
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("row_seq"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)

metrics = (
    enriched
    .filter(F.col("row_seq").between(4, 7))
    .select(
        "id",
        "is_flagged",
        "row_seq",
        F.ntile(10).over(w3).alias("running_amount_bucket"),
        F.ntile(5).over(w3).alias("running_amount_bucket_2"),
        F.sum("row_seq").over(w4).alias("row_seq_sum_30d"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "is_flagged",
        "running_amount_bucket_2",
        F.rank().over(w5).alias("running_amount_bucket_2_rank"),
        F.lag("running_amount_bucket_2", 1).over(w6).alias("prev_running_amount_bucket_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("prev_running_amount_bucket_2") >= 6)
    .withColumn("running_amount_bucket_2_scaled", F.round((F.col("running_amount_bucket_2") * 1.1), 2))
)

base = (
    valid
    .filter(F.col("running_amount_bucket_2").between(4, 7))
    .select(
        "*",
        (F.col("running_amount_bucket_2_scaled") * F.col("running_amount_bucket_2")).alias("running_amount_bucket_2_scaled_times_running_amount_bucket_2"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

result = (
    base
    .withColumn("prev_running_amount_bucket_2_tier", F.when(F.col("prev_running_amount_bucket_2") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("running_amount_bucket_2_scaled_tier", F.when(F.col("running_amount_bucket_2_scaled") >= 83, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["prev_running_amount_bucket_2_tier", "running_amount_bucket_2_scaled_tier"], ["prev_running_amount_bucket_2_tier"]],
        "prev_running_amount_bucket_2_tier",
        "running_amount_bucket_2_scaled_tier",
    )
    .agg(
        F.percentile_approx("running_amount_bucket_2", [0.25, 0.5, 0.75]).alias("running_amount_bucket_2_quartiles"),
        F.round(F.avg("running_amount_bucket_2_scaled_times_running_amount_bucket_2"), 2).alias("avg_running_amount_bucket_2_scaled_times_running_amount_bucket_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("running_amount_bucket_2_scaled_tier").desc())
)
