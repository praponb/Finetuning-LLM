from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 26)
    .withColumn("amount_adjusted", F.round((F.col("amount") * 100), 2))
    .withColumn("amount_with_shipping", (F.col("shipping_fee") - F.col("amount")))
)
