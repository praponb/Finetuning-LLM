from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("paid_at"), "status").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "paid_at"))
w3 = Window.partitionBy("status").orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("event_seq").desc())
w5 = Window.partitionBy("status")
w6 = Window.orderBy("first_known_event_seq", "id")
w7 = Window.orderBy(F.col("event_seq_dense_rank").desc(), "id")
w8 = Window.orderBy("first_known_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("event_seq").desc_nulls_first())
w10 = Window.orderBy("event_seq_dense_rank", "id")
w11 = Window.orderBy("first_known_event_seq_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("event_seq_rank", "id")

with_windows = (
    df_payments
    .select(
        "id",
        "status",
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "event_seq",
        F.first("event_seq", ignorenulls=True).over(w3).alias("first_known_event_seq"),
        F.dense_rank().over(w4).alias("event_seq_dense_rank"),
        F.avg("amount_share").over(w5).alias("status_avg_amount_share"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "event_seq_dense_rank",
        "status_avg_amount_share",
        F.ntile(3).over(w6).alias("first_known_event_seq_bucket"),
        F.ntile(3).over(w7).alias("event_seq_dense_rank_bucket"),
        F.sum("event_seq_dense_rank").over(w8).alias("cumulative_event_seq_dense_rank"),
        F.rank().over(w9).alias("event_seq_rank"),
        F.ntile(5).over(w10).alias("event_seq_dense_rank_bucket_2"),
    )
)

metrics_payments = (
    enriched
    .select(
        "*",
        F.sum("cumulative_event_seq_dense_rank").over(w11).alias("cumulative_cumulative_event_seq_dense_rank"),
        F.lag("cumulative_event_seq_dense_rank", 1).over(w10).alias("prev_cumulative_event_seq_dense_rank"),
        F.ntile(4).over(w_running).alias("event_seq_rank_bucket"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    metrics_payments
    .withColumn("event_seq_dense_rank_tier", F.when(F.col("event_seq_dense_rank") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("event_seq_dense_rank_bucket_tier", F.when(F.col("event_seq_dense_rank_bucket") >= 6, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["event_seq_dense_rank_tier", "event_seq_dense_rank_bucket_tier"], ["event_seq_dense_rank_tier"]],
        "event_seq_dense_rank_tier",
        "event_seq_dense_rank_bucket_tier",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("first_known_event_seq_bucket", 0.5).alias("first_known_event_seq_bucket_p50"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("first_known_event_seq_bucket_p50")
)
