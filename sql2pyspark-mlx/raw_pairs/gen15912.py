from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "attr_key")
w2 = Window.partitionBy("service")
w3 = Window.orderBy(F.col("status_code").desc())
w4 = Window.orderBy(F.col("status_code_cume_dist_per_status_code").desc())
w5 = Window.orderBy(F.col("service_avg_status_code_share").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

with_windows = (
    flattened
    .filter(F.col("service").isin(["search", "billing", "checkout"]))
    .select(
        "id",
        "service",
        "status_code",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share_2"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
    )
)

metrics = (
    with_windows
    .select(
        "status_code",
        "status_code_share",
        "status_code_cume_dist",
        F.avg("status_code_share").over(w2).alias("service_avg_status_code_share"),
    )
)

cleaned = (
    metrics
    .filter(F.col("status_code_share").isNotNull())
    .withColumn("status_code_cume_dist_per_status_code", F.round((F.col("status_code_cume_dist") / F.col("status_code")), 2))
)

with_windows_app_logs = (
    cleaned
    .select(
        "status_code_share",
        "status_code_cume_dist",
        "service_avg_status_code_share",
        F.cume_dist().over(w4).alias("status_code_cume_dist_per_status_code_cume_dist"),
        F.dense_rank().over(w5).alias("service_avg_status_code_share_dense_rank"),
    )
)

result = (
    with_windows_app_logs
    .rollup(
        F.when(F.col("service_avg_status_code_share_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("service_avg_status_code_share_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("status_code_share", 0.9).alias("status_code_share_p90"),
    )
    .filter(F.col("num_rows") > 1)
)
