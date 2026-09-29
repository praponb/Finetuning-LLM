from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc_nulls_first())
w3 = Window.partitionBy("service").orderBy(F.col("least_status_code_high_has_fault").desc_nulls_first())
w4 = Window.partitionBy("service").orderBy(F.col("least_status_code_high_has_fault").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

unpacked = (
    exploded
    .select(
        "id",
        "service",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    unpacked
    .filter(F.col("service").isin(["checkout", "search", "billing"]))
    .select(
        "*",
        F.least("status_code_high", "has_fault").alias("least_status_code_high_has_fault"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("least_status_code_high_has_fault").over(w).alias("service_sum_least_status_code_high_has_fault"),
        F.dense_rank().over(w2).alias("status_code_high_dense_rank"),
        F.avg("least_status_code_high_has_fault").over(w).alias("service_avg_least_status_code_high_has_fault"),
        F.percent_rank().over(w3).alias("least_status_code_high_has_fault_percent_rank"),
        F.cume_dist().over(w4).alias("least_status_code_high_has_fault_cume_dist"),
    )
)

summary = (
    metrics
    .withColumn("least_status_code_high_has_fault_percent_rank_tier", F.when(F.col("least_status_code_high_has_fault_percent_rank") >= 0.67, "PREMIUM").otherwise("STANDARD"))
    .rollup("least_status_code_high_has_fault_percent_rank_tier", "service")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.sum("service_sum_least_status_code_high_has_fault").alias("total_service_sum_least_status_code_high_has_fault"),
        F.round(F.sum("status_code_high_dense_rank") / F.nullif(F.count(F.when(F.col("service") == "auth", 1)), F.lit(0)), 2).alias("status_code_high_dense_rank_per_auth"),
        F.sum(F.when(F.col("service") == "auth", F.col("least_status_code_high_has_fault_percent_rank")).otherwise(0)).alias("auth_least_status_code_high_has_fault_percent_rank"),
        F.percentile_approx("has_fault", [0.25, 0.5, 0.75]).alias("has_fault_quartiles"),
        F.grouping("least_status_code_high_has_fault_percent_rank_tier").alias("grouping_id"),
    )
    .filter(F.col("total_service_sum_least_status_code_high_has_fault") < 0)
)

result = (
    summary
    .select(
        "least_status_code_high_has_fault_percent_rank_tier",
        "service",
        "service_list",
        "total_service_sum_least_status_code_high_has_fault",
        "status_code_high_dense_rank_per_auth",
        "auth_least_status_code_high_has_fault_percent_rank",
        "has_fault_quartiles",
        "grouping_id",
    )
)
