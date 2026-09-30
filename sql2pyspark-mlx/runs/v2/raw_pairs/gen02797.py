from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/checkout") & ((F.col("event_type") == "click") | (F.col("event_time") >= "2023-12-01")))
    .groupBy("event_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
    .filter(F.col("click_count") < 30)
)
