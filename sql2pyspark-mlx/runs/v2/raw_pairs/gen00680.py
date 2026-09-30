from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.max("discount").alias("max_discount"),
        F.count("shipping_fee").alias("shipping_fee_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("ct.max_discount"),
        F.col("ct.shipping_fee_count"),
    )
    .orderBy("name")
)
