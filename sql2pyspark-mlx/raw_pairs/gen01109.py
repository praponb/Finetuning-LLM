from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy(F.col("amount_rounded").desc(), "id")
w_ordered = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("running_avg_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("running_min_amount", "id")
w5 = Window.partitionBy("order_id", "method").orderBy("running_max_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method", "order_id").orderBy(F.col("next_amount").asc_nulls_last())
w7 = Window.orderBy("running_max_running_min_running_max_amount", "id")
w8 = Window.orderBy(F.col("next_amount_rank").desc(), "id")
w9 = Window.orderBy("running_sum_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("running_count_running_min_running_max_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-08-05").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.round("amount", 0).alias("amount_rounded"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.sum("amount").over(w_ordered).alias("running_sum_amount"),
        F.avg("amount").over(w_ordered).alias("running_avg_amount"),
        F.min("amount").over(w_ordered).alias("running_min_amount"),
        F.max("amount").over(w_ordered).alias("running_max_amount"),
    )
)

scored = (
    metrics
    .filter(F.col("amount_rounded") < 38)
    .select(
        "id",
        "order_id",
        "method",
        "latest_rank",
        "running_sum_amount",
        "running_avg_amount",
        "running_min_amount",
        "running_max_amount",
        F.min("running_max_amount").over(w3).alias("running_min_running_max_amount"),
        F.max("running_max_amount").over(w3).alias("running_max_running_max_amount"),
        F.count("running_max_amount").over(w3).alias("running_count_running_max_amount"),
        F.lead("amount", 2).over(w4).alias("next_amount"),
    )
)

metrics_payments = (
    scored
    .select(
        "id",
        "running_sum_amount",
        "running_avg_amount",
        "running_min_running_max_amount",
        "running_count_running_max_amount",
        F.count("running_min_running_max_amount").over(w5).alias("running_count_running_min_running_max_amount"),
        F.avg("running_min_running_max_amount").over(w5).alias("running_avg_running_min_running_max_amount"),
        F.max("running_min_running_max_amount").over(w5).alias("running_max_running_min_running_max_amount"),
        F.rank().over(w6).alias("next_amount_rank"),
    )
)

with_windows = (
    metrics_payments
    .select(
        "id",
        "running_avg_amount",
        "running_count_running_max_amount",
        "running_count_running_min_running_max_amount",
        "running_avg_running_min_running_max_amount",
        "next_amount_rank",
        F.lead("running_count_running_max_amount", 1).over(w7).alias("next_running_count_running_max_amount"),
        F.row_number().over(w8).alias("seq_no"),
        F.first("running_avg_amount", ignorenulls=True).over(w9).alias("first_known_running_avg_amount"),
        F.sum("running_avg_running_min_running_max_amount").over(w10).alias("cumulative_running_avg_running_min_running_max_amount"),
        F.ntile(5).over(w8).alias("next_amount_rank_bucket"),
    )
)

result = (
    with_windows
    .groupBy("seq_no")
    .agg(
        F.max("running_count_running_max_amount").alias("max_running_count_running_max_amount"),
        F.round(F.avg("next_amount_rank_bucket"), 2).alias("avg_next_amount_rank_bucket"),
        F.percentile_approx("cumulative_running_avg_running_min_running_max_amount", [0.25, 0.5, 0.75]).alias("cumulative_running_avg_running_min_running_max_amount_quartiles"),
    )
    .filter(F.col("max_running_count_running_max_amount") > 0)
    .orderBy(F.col("max_running_count_running_max_amount").desc())
)
