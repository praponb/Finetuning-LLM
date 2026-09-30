from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "CANCELLED", "SHIPPED"]))
    .groupBy(F.year("order_date").alias("order_year"), "channel")
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
)
