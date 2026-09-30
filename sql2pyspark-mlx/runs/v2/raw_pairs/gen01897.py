from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .agg(F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"))
    .filter(F.col("web_count") < 3)
)
