from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
    .orderBy("duration_sec_count")
)
