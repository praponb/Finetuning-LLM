from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = exploded.select("level", "status_code", "key", "value")
