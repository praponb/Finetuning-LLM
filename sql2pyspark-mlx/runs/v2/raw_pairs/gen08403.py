from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy(F.col("min_duration_sec").desc())
)
