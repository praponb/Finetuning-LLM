from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "purchase"]) & (F.to_date("event_time") >= "2023-09-22"))
    .groupBy(
        F.when(F.col("page").isin(["/checkout", "/cart"]), "group_a").otherwise("other").alias("page_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy(F.col("page_group").desc())
    .limit(50)
)
