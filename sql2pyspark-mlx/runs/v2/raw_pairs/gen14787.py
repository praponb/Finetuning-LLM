from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "customer_id",
        (F.col("shipping_fee") + F.col("amount")).alias("amount_with_shipping"),
    )
    .orderBy("customer_id", "store_id")
)
