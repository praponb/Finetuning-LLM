from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") <= 24) & (F.col("amount") > 440))
    .withColumn("discount_minus_amount", (F.col("discount") - F.col("amount")))
)
