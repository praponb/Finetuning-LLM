from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.year(F.col("customers.signup_date")) == 2023)
    .select(
        F.col("shipments.shipped_date"),
        F.col("shipments.status"),
        F.col("orders.customer_id"),
        F.col("orders.discount"),
        F.col("customers.email"),
        F.col("customers.is_active"),
    )
)
