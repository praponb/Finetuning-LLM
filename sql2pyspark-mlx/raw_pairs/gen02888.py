from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = flattened.select("host", "attr_key", "attr_value")
