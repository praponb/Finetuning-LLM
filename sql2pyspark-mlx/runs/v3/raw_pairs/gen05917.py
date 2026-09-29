from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") < 469) & F.col("page").isin(["/search", "/product", "/cart"]) & (F.to_date("event_time") >= "2023-10-14"))
    .groupBy("page")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
)
