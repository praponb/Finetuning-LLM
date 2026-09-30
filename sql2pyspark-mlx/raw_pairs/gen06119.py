from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.month("event_time").alias("event_month"),
        F.when(F.col("page").isin(["/search", "/home"]), "tier_1").otherwise("other").alias("page_group"),
    )
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") < 350)
    .orderBy(F.col("event_month").desc(), F.col("avg_duration_sec").desc())
)
