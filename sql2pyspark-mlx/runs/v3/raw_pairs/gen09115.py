from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 423)
    .groupBy(
        F.when(F.col("event_type").isin(["purchase", "click"]), "priority").otherwise("other").alias("event_type_group"),
    )
    .agg(
        F.count("*").alias("web_event_count"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
    )
    .filter(F.col("web_event_count") < 4)
    .orderBy(F.col("web_event_count").desc(), "event_type_group")
)
