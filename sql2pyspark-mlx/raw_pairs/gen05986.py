from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "web") & (F.col("order_date") <= "2024-11-01"))
    .withColumn("order_date_period", F.date_trunc("month", "order_date"))
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 33, "High").when(F.col("shipping_fee") >= 18, "Medium").otherwise("Low"))
)
