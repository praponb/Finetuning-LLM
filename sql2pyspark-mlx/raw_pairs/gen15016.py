from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
    )
    .filter(F.col("num_services") > 1)
    .orderBy(F.col("num_services").desc())
)
