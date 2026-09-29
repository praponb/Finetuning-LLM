from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "status",
        "customer_id",
        F.round((F.col("shipping_fee") / F.col("amount")), 2).alias("amount_with_shipping"),
    )
)
