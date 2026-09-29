from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.lit("order").alias("origin"), "shipping_fee")
    .union(df_payments.select(F.lit("payment").alias("origin"), "amount"))
    .distinct()
    .orderBy("shipping_fee")
)
