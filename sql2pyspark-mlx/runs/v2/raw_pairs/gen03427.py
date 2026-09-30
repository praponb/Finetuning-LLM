from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "CANCELLED"]) & F.col("store_id").isNotNull() & (F.col("status") == "SHIPPED"))
    .select(
        "id",
        "status",
        "store_id",
        (F.col("discount") - F.col("shipping_fee")).alias("discount_minus_shipping_fee"),
    )
)
