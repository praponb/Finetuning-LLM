from pyspark.sql import functions as F

result = (
    df_web_events
    .rollup("session_id", "event_type")
    .agg(
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc_nulls_first())
)
