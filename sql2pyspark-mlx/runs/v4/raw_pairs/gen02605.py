from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_first())
w_recent = Window.partitionBy("service")
w3 = Window.orderBy(F.col("status_code_adjusted").asc_nulls_first())
w4 = Window.partitionBy("host", "key")
w5 = Window.partitionBy("host").orderBy(F.col("status_code_adjusted_cume_dist").desc())
w6 = Window.partitionBy("service").orderBy("host_max_status_code_adjusted")
w7 = Window.partitionBy("service").orderBy(F.col("latency_ms_rank").asc_nulls_first())
w_running = Window.partitionBy("key").orderBy(F.col("status_code_adjusted_cume_dist").desc_nulls_last())
w9 = Window.partitionBy("service").orderBy(F.col("service_max_latency_ms").desc_nulls_last())
w10 = Window.partitionBy("level")
w11 = Window.partitionBy("key").orderBy(F.col("status_code_adjusted_cume_dist_cume_dist").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

valid = (
    exploded
    .filter((F.col("log_ts") >= F.lit("2025-06-01 08:00:00").cast("timestamp")) & F.col("key").isNotNull())
    .select(
        "service",
        "host",
        "level",
        "latency_ms",
        "key",
        F.lower("host").alias("host_lower"),
        F.round((F.col("status_code") * 0.85), 2).alias("status_code_adjusted"),
    )
)

scored = (
    valid
    .withColumn("latency_ms_rank", F.rank().over(w))
    .withColumn("service_max_latency_ms", F.max("latency_ms").over(w_recent))
    .withColumn("status_code_adjusted_cume_dist", F.cume_dist().over(w3))
    .withColumn("host_max_status_code_adjusted", F.max("status_code_adjusted").over(w4))
)

with_windows = (
    scored
    .select(
        "*",
        F.cume_dist().over(w5).alias("status_code_adjusted_cume_dist_cume_dist"),
        F.cume_dist().over(w6).alias("host_max_status_code_adjusted_cume_dist"),
        F.rank().over(w7).alias("latency_ms_rank_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w_running).alias("status_code_adjusted_cume_dist_cume_dist_2"),
        F.percent_rank().over(w9).alias("service_max_latency_ms_percent_rank"),
        F.round((F.col("service_max_latency_ms") / F.sum("service_max_latency_ms").over(w10)), 4).alias("service_max_latency_ms_share"),
        F.percent_rank().over(w11).alias("status_code_adjusted_cume_dist_cume_dist_percent_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("key").isNotNull())
    .withColumn("service_max_latency_ms_per_service_max_latency_ms_percent_rank", (F.col("service_max_latency_ms") / F.nullif("service_max_latency_ms_percent_rank", F.lit(0))))
    .withColumn("level_upper", F.upper("level"))
)

result = (
    filtered
    .withColumn("status_code_adjusted_tier", F.when(F.col("status_code_adjusted") >= 68, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["level", "status_code_adjusted_tier"], ["level"]],
        "level",
        "status_code_adjusted_tier",
    )
    .agg(
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.max(F.when(F.col("service") == "billing", F.col("status_code_adjusted"))).alias("billing_max_status_code_adjusted"),
        F.countDistinct("level").alias("distinct_levels"),
        F.max(F.when(F.col("host") == "h3", F.col("status_code_adjusted_cume_dist_cume_dist_2"))).alias("h3_max_status_code_adjusted_cume_dist_cume_dist_2"),
        F.grouping("level").alias("grouping_level"),
    )
    .orderBy(
        F.col("h3_max_status_code_adjusted_cume_dist_cume_dist_2").desc_nulls_last(),
    )
)
