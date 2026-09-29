from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("session_id") == 2)
    .withColumn("page_group", F.when(F.col("page").isin(["/checkout", "/product"]), "Tier 1").otherwise("Other"))
)

web_events_stats = (
    src
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    web_events_stats
    .filter(F.col("search_count") >= 65)
    .select("event_type", "search_count", "latest_event_time")
    .orderBy("event_type")
)
