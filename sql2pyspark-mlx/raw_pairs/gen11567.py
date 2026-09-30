from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/cart", "/search"]) & F.col("page").isin(["/home", "/cart", "/checkout"]))
    .withColumn("page_event_type_key", F.concat_ws("-", "page", "event_type"))
    .withColumn("event_time_period", F.date_trunc("month", "event_time"))
)
