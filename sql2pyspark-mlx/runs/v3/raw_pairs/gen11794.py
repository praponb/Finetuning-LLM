from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "purchase") & F.col("event_type").isin(["page_view", "add_to_cart", "search"]) & (F.col("duration_sec") > 158))
    .groupBy(
        "page",
        F.date_format("event_time", "yyyy-MM").alias("event_month"),
        F.quarter("event_time").alias("event_quarter"),
    )
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.min("event_time").alias("first_event_time"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
    .filter(F.col("max_duration_sec") < 310)
)
