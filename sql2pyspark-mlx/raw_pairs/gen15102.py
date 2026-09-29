from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "context",
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.size("context").alias("num_context"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("num_context") >= 88, "LARGE").otherwise("SMALL").alias("num_context_tier"),
    )
    .agg(
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
)
