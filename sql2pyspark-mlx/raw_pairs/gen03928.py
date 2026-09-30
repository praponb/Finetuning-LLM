from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(19, 25))
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.count("discount").alias("discount_count"))
)
