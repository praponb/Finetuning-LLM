from pyspark.sql import functions as F

base = df_web_events.filter(F.col("event_type") == "add_to_cart")

summary = (
    base
    .groupBy("page")
    .agg(F.count("*").alias("web_event_count"))
)

result = (
    summary
    .select("page", "web_event_count")
    .orderBy(F.col("page").desc())
)
