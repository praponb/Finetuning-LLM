from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "device",
        "customer_id",
        "country",
        F.concat("country", F.lit(", "), "device").alias("country_device"),
    )
)
