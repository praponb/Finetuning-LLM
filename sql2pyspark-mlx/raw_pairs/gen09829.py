from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([10, 21]) & F.col("event_type").isin(["search", "add_to_cart"]) & (F.to_date("event_time") >= "2025-01-08"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .orderBy(F.col("event_type_values").desc())
)
