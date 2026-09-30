from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "service", "level", F.explode_outer("context").alias("k", "value"))
)

result = (
    exploded
    .rollup("level")
    .agg(
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_rows"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_rows"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.sort_array(F.collect_set("service")).alias("service_list_2"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
    )
    .filter(F.col("warn_rows") > 1)
)
