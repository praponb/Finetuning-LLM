from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(11, 17) & (F.col("session_id").isin([1, 33]) | (F.col("duration_sec") < 451)))
    .groupBy("event_type")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("num_web_events"),
    )
    .orderBy(F.col("event_type").desc())
)
