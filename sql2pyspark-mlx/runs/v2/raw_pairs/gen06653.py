from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("page") == "/product") & (F.col("session_id").isin([18, 26, 27, 35]) | F.col("session_id").isin([10, 37]))))
    .groupBy("event_type")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
)
