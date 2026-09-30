from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("device")
    .agg(
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.countDistinct("device").alias("distinct_devices"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.min("started_at").alias("first_started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.max("started_at").alias("last_started_at"),
    )
)
