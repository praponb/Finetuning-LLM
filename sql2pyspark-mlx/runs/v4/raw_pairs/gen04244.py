from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNull() | F.col("session_id").isin([9, 10]))
    .groupBy("page")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
    .filter(F.col("duration_sec_count") < 3)
    .orderBy("click_count", "page")
    .limit(1)
)
