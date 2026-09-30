from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2025-04-01") & F.col("event_type").isin(["purchase", "page_view", "add_to_cart"]) & F.col("page").isin(["/checkout", "/cart", "/search"]))
    .groupBy("page")
    .agg(F.count("*").alias("cnt"))
)
