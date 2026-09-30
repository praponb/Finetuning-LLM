from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNull())
    .select("id", "amount", F.col("shipping_fee").cast("int").alias("shipping_fee_int"))
)
