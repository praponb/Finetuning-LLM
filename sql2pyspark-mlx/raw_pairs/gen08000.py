from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("adjusted_status_code")
w_hist_2 = Window.partitionBy("k")
w3 = Window.partitionBy("k").orderBy("adjusted_status_code")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

cleaned = (
    exploded
    .filter(F.col("status_code") >= 274)
    .select(
        "id",
        "latency_ms",
        "status_code",
        "k",
        "v",
        F.lower("host").alias("host_lower"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("k").isNotNull() & (F.col("status_code") >= 332))
    .withColumn("adjusted_status_code", F.round((F.col("status_code") * 1.1), 2))
    .withColumn("status_code_times_latency_ms", (F.col("status_code") * F.col("latency_ms")))
)

enriched = (
    cleaned_app_logs
    .filter(F.col("adjusted_status_code") <= 56)
    .withColumn("adjusted_status_code_rank", F.rank().over(w_hist))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w_hist_2))
    .withColumn("k_avg_latency_ms", F.avg("latency_ms").over(w_hist_2))
    .withColumn("adjusted_status_code_cume_dist", F.cume_dist().over(w3))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w_hist_2)), 4))
)

daily = (
    enriched
    .cube(
        F.when(F.col("corr_latency_ms_status_code") >= 0.51, "LARGE").otherwise("SMALL").alias("corr_latency_ms_status_code_tier"),
        "k",
    )
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.round(F.sum("adjusted_status_code") / F.nullif(F.count(F.when(F.col("k") == "region", 1)), F.lit(0)), 2).alias("adjusted_status_code_per_region"),
        F.round(F.sum("k_avg_latency_ms") / F.nullif(F.count(F.when(F.col("k") == "region", 1)), F.lit(0)), 2).alias("k_avg_latency_ms_per_region"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows_2"),
        F.avg("k_avg_latency_ms").alias("avg_k_avg_latency_ms"),
    )
)

result = (
    daily
    .select(
        "corr_latency_ms_status_code_tier",
        "num_ks",
        "adjusted_status_code_per_region",
        "k_avg_latency_ms_per_region",
        "trace_rows_2",
        "avg_k_avg_latency_ms",
    )
    .orderBy(F.col("avg_k_avg_latency_ms").desc(), F.col("trace_rows_2").desc())
)
