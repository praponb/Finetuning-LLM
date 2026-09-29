from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.length(F.col("customers.email")) < 7)
    .select(
        F.col("orders.shipping_fee"),
        F.col("orders.store_id"),
        F.col("orders.status"),
        F.col("customers.segment"),
        F.col("customers.is_active"),
        F.col("customers.name"),
    )
    .orderBy(F.col("name").desc(), "segment")
)
