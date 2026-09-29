from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select(
        "discount",
        "customer_id",
        "shipping_fee",
        F.floor("shipping_fee").alias("shipping_fee_whole"),
    )
    .orderBy("shipping_fee_whole")
)
