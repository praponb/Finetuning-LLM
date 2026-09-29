from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .withColumn("amount_with_shipping", (F.col("amount") + F.col("shipping_fee")))
    .orderBy(F.col("amount_with_shipping").desc())
)
