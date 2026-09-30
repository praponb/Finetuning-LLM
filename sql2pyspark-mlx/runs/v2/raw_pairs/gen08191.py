from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 20) & F.col("discount").between(0.25, 0.38))
    .select(
        "id",
        "store_id",
        "order_date",
        F.greatest("discount", "shipping_fee").alias("greatest_discount_shipping_fee"),
    )
)
