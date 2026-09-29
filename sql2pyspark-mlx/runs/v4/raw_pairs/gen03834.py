from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-04-21 15:00:00").cast("timestamp"))
    .withColumn("event_type_lower", F.lower("event_type"))
    .withColumn("event_type_prefix", F.substring("event_type", 1, 4))
)

result = (
    cleaned
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("event_time") >= "2023-11-01")
        .select("id")
    )
)
