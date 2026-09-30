from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2024-02-21 09:00:00").cast("timestamp"))
    .withColumn("country_device_key", F.concat_ws("-", "country", "device"))
)

result = (
    valid
    .select("country", "customer_id")
    .intersectAll(
        df_sessions
        .filter(F.col("customer_id").isin([1, 34]))
        .select("country", "customer_id")
    )
)
