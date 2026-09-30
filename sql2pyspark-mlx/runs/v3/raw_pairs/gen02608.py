from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("shipping_fee").between(27, 36) | (F.col("shipping_fee") >= 39)))
    .withColumn("amount_with_shipping", (F.col("amount") + F.col("shipping_fee")))
)
