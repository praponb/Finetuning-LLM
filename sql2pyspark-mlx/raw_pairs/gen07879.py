from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "k").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k", "service").orderBy("latency_ms_avg_5", "id", "k").rowsBetween(-6, -1)
w5 = Window.partitionBy("k").orderBy(F.col("running_latency_ms").desc(), "id", "k")
w_hist = Window.partitionBy("k").orderBy("running_latency_ms_avg_6_prior", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("k").orderBy(F.col("seq_no").desc(), "id", "k")
w8 = Window.orderBy(F.col("running_latency_ms_avg_6_prior").desc_nulls_last())
w9 = Window.partitionBy("k").orderBy("row_seq", "id", "k")
w_part = Window.partitionBy("k")
w11 = Window.partitionBy("k").orderBy("first_known_running_latency_ms", "id", "k")
w12 = Window.partitionBy("k").orderBy(F.col("row_seq").desc(), "id", "k")
w13 = Window.partitionBy("k").orderBy("running_sum_first_known_running_latency_ms", "id", "k")
w14 = Window.orderBy("event_seq", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

enriched = (
    exploded
    .select(
        "id",
        "service",
        "k",
        "attr_value",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_5"),
        F.sum("latency_ms").over(w2).alias("running_latency_ms"),
    )
)

enriched_app_logs = (
    enriched
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "k",
        "attr_value",
        "latency_ms_avg_5",
        F.first("running_latency_ms", ignorenulls=True).over(w3).alias("first_known_running_latency_ms"),
        F.avg("running_latency_ms").over(w4).alias("running_latency_ms_avg_6_prior"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

metrics = (
    enriched_app_logs
    .withColumn("running_min_first_known_running_latency_ms", F.min("first_known_running_latency_ms").over(w_hist))
    .withColumn("running_sum_first_known_running_latency_ms", F.sum("first_known_running_latency_ms").over(w_hist))
    .withColumn("running_count_first_known_running_latency_ms", F.count("first_known_running_latency_ms").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w7))
)

scored = (
    metrics
    .select(
        "*",
        F.rank().over(w8).alias("running_latency_ms_avg_6_prior_rank"),
        F.row_number().over(w9).alias("row_seq_2"),
        F.ntile(10).over(w9).alias("row_seq_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "k",
        "attr_value",
        "latency_ms_avg_5",
        "running_latency_ms_avg_6_prior",
        "running_sum_first_known_running_latency_ms",
        "running_count_first_known_running_latency_ms",
        F.min("running_latency_ms_avg_6_prior_rank").over(w_part).alias("k_min_running_latency_ms_avg_6_prior_rank"),
        F.ntile(10).over(w11).alias("first_known_running_latency_ms_bucket"),
        F.row_number().over(w12).alias("event_seq"),
    )
)

enriched_app_logs_2 = (
    with_windows
    .withColumn("running_sum_first_known_running_latency_ms_bucket", F.ntile(5).over(w13))
    .withColumn("event_seq_bucket", F.ntile(4).over(w14))
    .withColumn("k_sum_running_sum_first_known_running_latency_ms", F.sum("running_sum_first_known_running_latency_ms").over(w_part))
)

result = (
    enriched_app_logs_2
    .select(
        "id",
        "k",
        "attr_value",
        "latency_ms_avg_5",
        "running_sum_first_known_running_latency_ms",
        "running_count_first_known_running_latency_ms",
        "first_known_running_latency_ms_bucket",
        "event_seq_bucket",
        "k_sum_running_sum_first_known_running_latency_ms",
    )
    .orderBy(F.col("id").desc_nulls_last(), "latency_ms_avg_5")
)
