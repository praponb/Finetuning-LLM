from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_ordered = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_last())
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w5 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.orderBy(F.col("log_ts_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("service").orderBy(F.col("log_ts_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.partitionBy("k")
w9 = Window.partitionBy("service", "k")
w10 = Window.partitionBy("host").orderBy(F.col("latency_ms_dense_rank").desc_nulls_last())
w11 = Window.partitionBy("k", "host").orderBy(F.col("log_ts_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w12 = Window.partitionBy("k").orderBy(F.col("latency_ms_dense_rank").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

scored = (
    exploded
    .withColumn("status_code_sum_1h", F.sum("status_code").over(w))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("latency_ms_max_2h", F.max("latency_ms").over(w3))
    .withColumn("status_code_dense_rank", F.dense_rank().over(w_running))
)

cleaned = (
    scored
    .filter(F.col("k").isNotNull())
    .select(
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        "status_code_sum_1h",
        "latency_ms_dense_rank",
        "latency_ms_max_2h",
        "status_code_dense_rank",
        F.date_trunc("year", "log_ts").alias("log_ts_period"),
        F.round((F.col("status_code_dense_rank") * 1.07), 2).alias("adjusted_status_code_dense_rank"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.max("status_code_sum_1h").over(w5).alias("status_code_sum_1h_max_30m"),
        F.avg("latency_ms").over(w6).alias("latency_ms_avg_7d"),
        F.count("adjusted_status_code_dense_rank").over(w7).alias("adjusted_status_code_dense_rank_count_7d"),
    )
)

scored_app_logs = (
    enriched
    .select(
        "*",
        F.min("latency_ms").over(w8).alias("k_min_latency_ms"),
        F.max("status_code_dense_rank").over(w9).alias("service_max_status_code_dense_rank"),
    )
)

filtered = (
    scored_app_logs
    .filter(F.col("k").isNotNull())
    .select(
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        "status_code_sum_1h",
        "latency_ms_dense_rank",
        "status_code_dense_rank",
        "log_ts_period",
        "adjusted_status_code_dense_rank",
        "status_code_sum_1h_max_30m",
        "k_min_latency_ms",
        F.initcap(F.trim("host")).alias("host_title"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("resolved_k"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.rank().over(w10).alias("latency_ms_dense_rank_rank"),
        F.count("latency_ms_dense_rank").over(w11).alias("latency_ms_dense_rank_count_30d"),
        F.rank().over(w12).alias("latency_ms_dense_rank_rank_2"),
        F.round((F.col("k_min_latency_ms") / F.sum("k_min_latency_ms").over(w8)), 4).alias("k_min_latency_ms_share"),
    )
)

summary = (
    metrics
    .rollup("k")
    .agg(
        F.count(F.when(F.col("k") == "version", 1)).alias("version_rows"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.sum(F.when(F.col("k") == "region", F.col("status_code_sum_1h")).otherwise(0)).alias("region_status_code_sum_1h"),
        F.sum(F.when(F.col("k") == "region", F.col("latency_ms_dense_rank_rank")).otherwise(0)).alias("region_latency_ms_dense_rank_rank"),
        F.percentile_approx("status_code_sum_1h", [0.25, 0.5, 0.75]).alias("status_code_sum_1h_quartiles"),
    )
    .filter(F.col("version_rows") >= 1)
)

result = (
    summary
    .select(
        "k",
        "version_rows",
        "region_status_code_sum_1h",
        "status_code_sum_1h_quartiles",
        F.when(F.col("k").rlike("^(trace|region)$"), "CORE").otherwise("OTHER_ZONE").alias("k_zone"),
    )
    .orderBy(F.col("region_status_code_sum_1h").desc())
)
