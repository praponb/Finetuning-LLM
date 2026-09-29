from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(6, 9) | (F.col("duration_sec") > 307))
    .withColumn("page_title", F.initcap(F.trim("page")))
    .withColumn("page_prefix", F.substring("page", 1, 4))
)
