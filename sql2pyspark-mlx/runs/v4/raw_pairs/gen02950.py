from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "service")
w2 = Window.partitionBy("service").orderBy("status_code")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("level").isin(["DEBUG", "ERROR", "WARN"]))
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "attr_value",
        F.lower("attr_key").alias("attr_key_lower"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.sum("status_code").over(w).alias("level_sum_status_code"),
        F.percent_rank().over(w2).alias("status_code_percent_rank"),
    )
)

daily = (
    metrics
    .groupBy(
        "level",
        F.when(F.col("level_sum_status_code") >= 1901, "LARGE").otherwise("SMALL").alias("level_sum_status_code_tier"),
    )
    .agg(
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.sum(F.when(F.col("service") == "auth", F.col("status_code")).otherwise(0)).alias("auth_status_code"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
)

result = (
    daily
    .select("level", "level_sum_status_code_tier", "status_code_p95", "service_list")
    .orderBy(F.col("level_sum_status_code_tier").asc_nulls_last(), "status_code_p95")
)
