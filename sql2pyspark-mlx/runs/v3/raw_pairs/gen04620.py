from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("session_id").isin([11, 20, 23, 39]) & F.col("session_id").isin([14, 33])) | F.hour("event_time").between(9, 13)))
    .groupBy("page")
    .agg(F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"))
)
