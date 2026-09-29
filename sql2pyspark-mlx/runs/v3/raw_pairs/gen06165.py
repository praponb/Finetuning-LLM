from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg((F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.shipping_fee_range") > 61)
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("cs.shipping_fee_range"),
    )
    .orderBy("id")
)
