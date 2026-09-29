from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 196)
    .withColumn("event_type_upper", F.upper("event_type"))
    .withColumn("page_event_type", F.concat("page", F.lit(" - "), "event_type"))
)
