from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/search") & ~F.col("event_type").isin(["add_to_cart", "search"]))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("max_duration_sec").desc())
)
