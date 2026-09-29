from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["page_view", "search"]))
    .groupBy("event_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
    )
    .orderBy("search_count", F.col("pct_home").desc())
)
