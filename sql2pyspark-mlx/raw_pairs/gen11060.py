from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("amount_per_shipping_fee", (F.col("amount") / F.nullif("shipping_fee", F.lit(0))))
    .orderBy("id")
)
