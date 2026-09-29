from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 23)
    .groupBy("channel")
    .agg(F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"))
)
