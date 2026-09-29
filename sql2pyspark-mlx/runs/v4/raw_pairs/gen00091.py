from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "device",
        "country",
        F.to_date("started_at").alias("started_at_date"),
        F.upper("device").alias("device_upper"),
    )
    .orderBy(F.col("device").desc())
)
