from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 22)
    .withColumn("least_shipping_fee_discount", F.least("shipping_fee", "discount"))
    .withColumn("amount_with_shipping", (F.col("shipping_fee") * F.col("amount")))
    .orderBy("status")
)
