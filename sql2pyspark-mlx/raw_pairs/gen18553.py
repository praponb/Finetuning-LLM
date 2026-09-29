from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service") == "checkout")
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "context",
        F.col("context")["region"].alias("region"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
