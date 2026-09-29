from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "mobile") & F.col("store_id").isNull())
    .select(
        "id",
        "discount",
        "order_date",
        "shipping_fee",
        F.floor("discount").alias("discount_whole"),
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)
