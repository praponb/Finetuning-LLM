from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/cart", "/product"]) & ~F.col("page").isin(["/home", "/search", "/product"]))
    .groupBy("session_id", "event_type")
    .agg(
        F.count("*").alias("row_count"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .filter(F.col("row_count") > 5)
)
