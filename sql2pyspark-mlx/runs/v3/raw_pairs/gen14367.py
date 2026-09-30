from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .select(
        "id",
        "order_date",
        "store_id",
        "status",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.ceil("shipping_fee").alias("shipping_fee_whole"),
    )
)
