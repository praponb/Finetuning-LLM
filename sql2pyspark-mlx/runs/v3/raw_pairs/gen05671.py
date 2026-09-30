from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 25, 30, 37]) | F.col("page").isin(["/home", "/checkout", "/cart"]))
    .groupBy("event_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.count("*").alias("row_count"),
    )
)
