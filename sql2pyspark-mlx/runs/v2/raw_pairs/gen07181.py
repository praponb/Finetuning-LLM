from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 18, 31, 40]))
    .select("event_type", F.upper("event_type").alias("event_type_upper"))
    .orderBy("event_type_upper", "event_type")
)
