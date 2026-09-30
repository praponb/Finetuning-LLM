from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select("id", "host", "status_code", "k", "attr_value")
    .orderBy("id")
)
