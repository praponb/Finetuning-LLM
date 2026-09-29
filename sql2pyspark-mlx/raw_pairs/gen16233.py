from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("months_since_event_time", (F.datediff(F.current_date(), "event_time") / 30))
)
