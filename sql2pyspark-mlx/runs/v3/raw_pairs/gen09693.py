from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/home"]))
    .cube("page", "event_type")
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)
