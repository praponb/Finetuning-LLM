from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-05-02"))) & F.col("page").isNotNull())
    .withColumn("duration_sec_band", F.when(F.col("duration_sec") >= 533, "High").when(F.col("duration_sec") >= 170, "Medium").otherwise("Low"))
)

result = (
    base
    .select("event_type", "id")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([13, 14, 17, 40]))
        .select("event_type", "id")
    )
)
