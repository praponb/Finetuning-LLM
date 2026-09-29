from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "page_view"]))
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .filter(F.col("pct_search") > 43)
    .select("event_quarter", "page_values", "pct_search")
    .orderBy(F.col("pct_search").desc())
)
