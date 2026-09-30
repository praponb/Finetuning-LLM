from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.order_date").between("2024-05-14", "2025-03-01"))
    .select(
        F.col("customers.is_active"),
        F.col("orders.order_date"),
        F.col("orders.status"),
        F.col("orders.discount"),
    )
    .orderBy(F.col("discount").desc())
)
