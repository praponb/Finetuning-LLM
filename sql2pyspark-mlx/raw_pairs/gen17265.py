from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2023-06-27")
    .select(
        "id",
        "service",
        "log_ts",
        F.col("context")["version"].alias("version"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["trace"].alias("trace"),
    )
)
