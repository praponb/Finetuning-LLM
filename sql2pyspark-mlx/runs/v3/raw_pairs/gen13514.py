from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([2, 3, 11]))
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        F.round((F.col("shipping_fee") * 0.85), 2).alias("shipping_fee_scaled"),
    )
)
