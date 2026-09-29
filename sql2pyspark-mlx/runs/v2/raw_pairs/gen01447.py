from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") < "2023-09-18") & F.col("store_id").isin([4, 39]))
    .select(
        "status",
        F.least("discount", "shipping_fee").alias("least_discount_shipping_fee"),
    )
    .orderBy("status")
)
