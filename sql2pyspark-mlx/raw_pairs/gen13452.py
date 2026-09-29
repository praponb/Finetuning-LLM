from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key")
w3 = Window.orderBy("status_code", "id", "attr_key")
w4 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key")
w_grp = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(-2, -1)
w6 = Window.partitionBy("host")
w7 = Window.partitionBy("attr_key").orderBy("running_min_latency_ms", "id", "attr_key")
w8 = Window.orderBy("prev_running_sum_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("attr_key").orderBy("running_count_latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("running_min_latency_ms", "id", "attr_key").rowsBetween(-6, -1)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

scored = (
    lines
    .filter(F.col("host").isin(["h1", "h4"]))
    .select(
        "id",
        "host",
        "status_code",
        "attr_key",
        "v",
        F.min("latency_ms").over(w_ordered).alias("running_min_latency_ms"),
        F.avg("latency_ms").over(w_ordered).alias("running_avg_latency_ms"),
        F.sum("latency_ms").over(w_ordered).alias("running_sum_latency_ms"),
        F.count("latency_ms").over(w_ordered).alias("running_count_latency_ms"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "attr_key",
        "running_min_latency_ms",
        "running_avg_latency_ms",
        "running_sum_latency_ms",
        "running_count_latency_ms",
        F.lag("running_avg_latency_ms", 1).over(w3).alias("prev_running_avg_latency_ms"),
        F.ntile(5).over(w4).alias("status_code_bucket"),
        F.avg("running_count_latency_ms").over(w_grp).alias("running_count_latency_ms_avg_2_prior"),
        F.avg("status_code").over(w6).alias("host_avg_status_code"),
        F.lag("running_sum_latency_ms", 1).over(w7).alias("prev_running_sum_latency_ms"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "attr_key",
        "running_min_latency_ms",
        "running_sum_latency_ms",
        "running_count_latency_ms",
        "status_code_bucket",
        "host_avg_status_code",
        F.last("attr_key", ignorenulls=True).over(w8).alias("last_known_attr_key"),
        F.sum("running_count_latency_ms_avg_2_prior").over(w9).alias("running_running_count_latency_ms_avg_2_prior"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "attr_key",
        "running_sum_latency_ms",
        "status_code_bucket",
        "last_known_attr_key",
        F.sum("running_min_latency_ms").over(w9).alias("running_running_min_latency_ms"),
        F.avg("running_min_latency_ms").over(w10).alias("running_min_latency_ms_avg_6_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "attr_key",
        "running_sum_latency_ms",
        "status_code_bucket",
        "running_running_min_latency_ms",
    )
    .orderBy(F.col("running_running_min_latency_ms").desc_nulls_last())
)
