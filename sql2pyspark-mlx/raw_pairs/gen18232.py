from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .groupBy("page")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .orderBy(F.col("page").desc())
)
