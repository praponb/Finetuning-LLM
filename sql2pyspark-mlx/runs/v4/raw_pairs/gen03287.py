from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = flattened.select("id", "host", "log_ts", "k")
