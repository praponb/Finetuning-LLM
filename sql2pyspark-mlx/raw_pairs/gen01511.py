from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id", "store_id")
    .agg(F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"))
    .filter(F.col("web_count") >= 29)
    .orderBy("web_count")
)
