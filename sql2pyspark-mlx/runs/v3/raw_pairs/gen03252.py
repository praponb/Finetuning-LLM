from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "search") & F.col("page").isin(["/search", "/home"]) & F.col("page").isin(["/cart", "/search"]))
    .groupBy("page")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
)
