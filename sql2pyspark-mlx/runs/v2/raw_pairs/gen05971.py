from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/product", "/cart"]) | (F.to_date("event_time") >= "2023-10-14"))
    .groupBy("event_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
    .filter(F.col("unique_event_types") < 5)
)
