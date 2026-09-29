from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("event_type_upper", F.upper("event_type"))
    .withColumn("event_time_hour", F.hour("event_time"))
    .orderBy("page")
)
