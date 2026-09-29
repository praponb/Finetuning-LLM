from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & ((F.col("store_id") == 7) | (F.col("amount") > 1450)))
    .select(
        "shipping_fee",
        "status",
        "customer_id",
        F.round((F.col("discount") * 0.85), 2).alias("discount_adjusted"),
    )
)
