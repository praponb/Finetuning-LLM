from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(24, 31))
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"))
    .filter(F.col("store_count") < 4)
)
