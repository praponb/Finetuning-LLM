from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") >= 31) & F.col("customer_id").isNotNull() & F.col("order_date").between("2024-07-16", "2025-09-01"))
    .withColumn("greatest_amount_shipping_fee", F.greatest("amount", "shipping_fee"))
    .withColumn("amount_with_shipping", (F.col("amount") - F.col("shipping_fee")))
)
