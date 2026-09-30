from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.year("order_date") >= 2025))
    .select(
        "id",
        "discount",
        "shipping_fee",
        F.when(F.col("status").isin(["PENDING", "CANCELLED"]), "Group A").otherwise("Other").alias("status_group"),
    )
)
