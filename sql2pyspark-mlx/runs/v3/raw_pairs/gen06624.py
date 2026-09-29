from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "started_at",
        "country",
        "device",
        F.length("device").alias("device_length"),
    )
    .orderBy(F.col("started_at").desc(), F.col("device_length").desc())
)
