from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    exploded
    .select("service", "log_ts", "v")
    .orderBy("log_ts")
)
