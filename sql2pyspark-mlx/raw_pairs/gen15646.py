from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["page_view", "search"]))
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
    .orderBy("page_values")
)
