from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "device",
        F.quarter("started_at").alias("started_at_quarter"),
        F.concat_ws("-", "device", "country").alias("device_country_key"),
    )
)
