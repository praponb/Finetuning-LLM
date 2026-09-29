from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 470, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("service") == "search", 1)), F.lit(0)), 2).alias("status_code_per_search"),
        F.sort_array(F.collect_set("k")).alias("k_list_2"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
    )
    .filter(F.col("duration_min") >= 0)
)
