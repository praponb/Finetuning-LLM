from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)
