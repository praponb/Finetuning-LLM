from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "service", "host", "k", "attr_value")
    .orderBy("host")
)
