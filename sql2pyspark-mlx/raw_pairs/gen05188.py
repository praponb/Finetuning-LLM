from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .orderBy(F.col("product_count").desc())
)
