from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "device",
        "country",
        "browser",
        F.concat("device", F.lit(", "), "country").alias("device_country"),
    )
)
