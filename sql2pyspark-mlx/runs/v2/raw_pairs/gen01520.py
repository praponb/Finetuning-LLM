from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") != "partner") & (F.col("amount") > 860))
    .select("customer_id", F.col("shipping_fee").cast("int").alias("shipping_fee_int"))
    .orderBy(F.col("shipping_fee_int").desc(), "customer_id")
)
