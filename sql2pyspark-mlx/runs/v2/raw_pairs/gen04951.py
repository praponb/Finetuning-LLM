from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "page", "session_id")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
    )
    .filter(F.col("min_duration_sec") < 210)
    .orderBy(F.col("page_values").desc())
)
