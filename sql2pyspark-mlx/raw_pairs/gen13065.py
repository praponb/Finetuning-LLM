from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["search", "click"]) & (F.col("duration_sec") <= 139))
    .groupBy("page")
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)
