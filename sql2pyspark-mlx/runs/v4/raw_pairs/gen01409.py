from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/search", "/home"]))
    .select("*", F.substring("page", 1, 4).alias("page_prefix"))
)

enriched = (
    valid
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.device"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.country"))
)

stats = (
    with_dims
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.countDistinct("page").alias("distinct_pages"),
    )
)

result = stats.select("week_start", "desktop_events", "distinct_pages")
