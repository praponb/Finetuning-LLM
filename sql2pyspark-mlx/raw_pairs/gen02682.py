from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("status_code_per_latency_ms").desc_nulls_first())
w3 = Window.partitionBy("k").orderBy(F.col("avg_cumulative_status_code_per_latency_ms").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

cleaned = (
    lines
    .filter(F.col("k") != "region")
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "k",
        F.substring("k", 1, 3).alias("k_prefix"),
        F.trim("host").alias("clean_host"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("resolved_k"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "latency_ms",
        "k",
        "resolved_k",
        F.round((F.col("status_code") / F.col("latency_ms")), 2).alias("status_code_per_latency_ms"),
    )
)

scored = (
    cleaned_app_logs
    .select(
        "id",
        "k",
        "resolved_k",
        F.first("latency_ms", ignorenulls=True).over(w).alias("first_known_latency_ms"),
        F.sum("status_code_per_latency_ms").over(w).alias("cumulative_status_code_per_latency_ms"),
        F.percent_rank().over(w2).alias("status_code_per_latency_ms_percent_rank"),
    )
)

summary = (
    scored
    .groupBy(
        "k",
        F.when(F.col("first_known_latency_ms") >= 2643, "PREMIUM").otherwise("STANDARD").alias("first_known_latency_ms_tier"),
    )
    .agg(
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
        F.max(F.when(F.col("k") == "trace", F.col("first_known_latency_ms"))).alias("trace_max_first_known_latency_ms"),
        F.percentile_approx("status_code_per_latency_ms_percent_rank", [0.25, 0.5, 0.75]).alias("status_code_per_latency_ms_percent_rank_quartiles"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows_2"),
        F.round(F.avg("cumulative_status_code_per_latency_ms"), 2).alias("avg_cumulative_status_code_per_latency_ms"),
        F.percentile_approx("first_known_latency_ms", [0.25, 0.5, 0.75]).alias("first_known_latency_ms_quartiles"),
    )
    .filter(F.col("avg_cumulative_status_code_per_latency_ms") < 1000000)
    .withColumn("avg_cumulative_status_code_per_latency_ms_dense_rank", F.dense_rank().over(w3))
)

result = (
    summary
    .select(
        "k",
        "first_known_latency_ms_tier",
        "region_rows",
        "trace_max_first_known_latency_ms",
        "region_rows_2",
        "avg_cumulative_status_code_per_latency_ms",
        "first_known_latency_ms_quartiles",
    )
)
