from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2024-08-10")
    .withColumn("page_prefix", F.substring("page", 1, 2))
    .withColumn("event_type_title", F.initcap(F.trim("event_type")))
    .orderBy("event_type")
)
