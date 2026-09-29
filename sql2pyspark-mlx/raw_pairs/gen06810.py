from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy(F.col("log_ts").desc(), "id", "k")
w3 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w4 = Window.partitionBy("level", "host").orderBy("row_seq", "id", "k").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("level").orderBy(F.col("prev_status_code").desc(), "id", "k")
w6 = Window.partitionBy("level").orderBy("last3_avg_prev_status_code_rounded", "id", "k").rowsBetween(-5, -1)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

with_windows = (
    flattened
    .filter(F.col("log_ts") < "2024-07-01")
    .select(
        "id",
        "host",
        "level",
        "k",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.lag("status_code", 1).over(w3).alias("prev_status_code"),
    )
)

base = (
    with_windows
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.round("prev_status_code", 1).alias("prev_status_code_rounded"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.min("prev_status_code_rounded").over(w4).alias("last3_min_prev_status_code_rounded"),
        F.avg("prev_status_code_rounded").over(w4).alias("last3_avg_prev_status_code_rounded"),
        F.count("prev_status_code_rounded").over(w4).alias("last3_count_prev_status_code_rounded"),
    )
)

metrics_app_logs = (
    metrics
    .select(
        "id",
        "host",
        "level",
        "k",
        "event_seq",
        F.ntile(3).over(w_recent).alias("prev_status_code_bucket"),
        F.avg("prev_status_code_rounded").over(w6).alias("prev_status_code_rounded_avg_5_prior"),
    )
)

valid = (
    metrics_app_logs
    .filter(F.col("k").isNotNull())
    .withColumn("greatest_prev_status_code_bucket_prev_status_code_rounded_avg_5_prior", F.greatest("prev_status_code_bucket", "prev_status_code_rounded_avg_5_prior"))
)

result = (
    valid
    .cube("host")
    .agg(
        F.percentile_approx("prev_status_code_bucket", 0.5).alias("prev_status_code_bucket_p50"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.grouping("host").alias("gid"),
    )
    .orderBy(F.col("host").desc())
)
