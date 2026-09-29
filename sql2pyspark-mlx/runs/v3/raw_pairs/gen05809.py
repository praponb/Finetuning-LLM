from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "purchase") | F.col("event_type").isin(["click", "search", "purchase"]))
    .groupBy(F.quarter("event_time").alias("event_quarter"), "session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
)
