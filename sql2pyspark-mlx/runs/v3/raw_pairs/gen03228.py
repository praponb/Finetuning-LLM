from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2024-08-10", "2025-01-17"))
    .select(
        "shipping_fee",
        "status",
        "store_id",
        (F.col("discount") * F.col("shipping_fee")).alias("discount_times_shipping_fee"),
        F.when(F.col("status").isin(["COMPLETED", "CANCELLED"]), "Group A").otherwise("Other").alias("status_group"),
    )
    .orderBy("discount_times_shipping_fee")
)
