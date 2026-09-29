from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-07-01")
    .withColumn("event_type_title", F.initcap(F.trim("event_type")))
    .withColumn("event_type_page_key", F.concat_ws("-", "event_type", "page"))
)
