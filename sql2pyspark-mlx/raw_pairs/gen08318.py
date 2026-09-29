from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 15)
    .select(F.lit("order").alias("kind"), "status")
    .unionByName(df_payments.select(F.lit("payment").alias("kind"), "status"))
    .orderBy("kind")
)
