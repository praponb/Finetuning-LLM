from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("session_id") == 9) & (F.col("session_id") == 30))
    .withColumn("duration_sec_num", F.col("duration_sec").cast("double"))
)

summary = (
    filtered_web_events
    .groupBy(
        F.year("event_time").alias("event_year"),
        F.hour("event_time").alias("event_hour"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec_num")).otherwise(0)).alias("cart_duration_sec_num"),
    )
)

result = (
    summary
    .filter(F.col("cart_duration_sec_num") < 50)
    .select("event_year", "event_hour")
    .orderBy("event_hour", F.col("event_year").desc())
)
