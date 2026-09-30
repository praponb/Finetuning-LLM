from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)
