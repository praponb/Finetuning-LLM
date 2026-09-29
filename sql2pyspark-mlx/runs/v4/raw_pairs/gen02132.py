from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("page").isin(["/product", "/home"]), "core").otherwise("other").alias("page_group"),
        F.quarter("event_time").alias("event_quarter"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("total_duration_sec") > 2400)
)
