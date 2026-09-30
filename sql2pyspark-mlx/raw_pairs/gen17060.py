from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("txn_time"), "account_id").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("txn_type").orderBy("amount_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("amount_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("amount").desc(), "id")
w6 = Window.partitionBy("txn_type").orderBy("amount", "id")
w_part = Window.partitionBy("account_id").orderBy("amount_avg_3", "id")
w8 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("event_seq").asc_nulls_last())
w_ordered = Window.partitionBy("merchant", "txn_type").orderBy("event_seq_2", "id")
w10 = Window.partitionBy("txn_type")

metrics = (
    df_transactions
    .withColumn("amount_avg_3", F.avg("amount").over(w))
    .withColumn("amount_bucket", F.ntile(4).over(w2))
)

enriched = (
    metrics
    .filter(F.col("amount_avg_3") > 3928)
    .select(
        "*",
        F.sum("amount").over(w3).alias("cumulative_amount"),
        F.sum("amount_bucket").over(w4).alias("running_amount_bucket"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("event_seq_2"),
        F.lag("amount_bucket", 1).over(w_part).alias("prev_amount_bucket"),
    )
)

filtered = (
    enriched
    .filter((F.col("txn_time") >= F.lit("2023-06-16").cast("date")) & F.col("account_id").isin([1, 7, 9, 24]))
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "amount_avg_3",
        "cumulative_amount",
        "event_seq",
        "event_seq_2",
        "prev_amount_bucket",
        (F.col("amount_bucket") * F.col("running_amount_bucket")).alias("amount_bucket_times_running_amount_bucket"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "amount",
        "merchant",
        "cumulative_amount",
        "event_seq_2",
        "amount_bucket_times_running_amount_bucket",
        F.rank().over(w8).alias("event_seq_rank"),
        F.lead("amount_avg_3", 1, 0).over(w_ordered).alias("next_amount_avg_3"),
        F.count("prev_amount_bucket").over(w10).alias("txn_type_count_prev_amount_bucket"),
    )
)

result = (
    with_windows
    .groupBy("merchant")
    .agg(
        F.percentile_approx("txn_type_count_prev_amount_bucket", [0.25, 0.5, 0.75]).alias("txn_type_count_prev_amount_bucket_quartiles"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows"),
    )
    .filter(F.col("starbucks_rows") >= 1)
    .orderBy(F.col("merchant").desc_nulls_last())
)
