from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 271)
    .groupBy("page", "event_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .orderBy("page", "event_type")
    .limit(50)
)
