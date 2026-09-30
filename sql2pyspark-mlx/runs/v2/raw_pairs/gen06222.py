from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page").isNull())
    .withColumn("event_type_page", F.concat("event_type", F.lit(", "), "page"))
)

summary = (
    base
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = summary.select("event_month", "max_duration_sec")
