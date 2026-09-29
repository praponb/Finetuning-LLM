from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("shipping_fee").between(14, 30))
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("order_date").alias("first_order_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.name"),
        F.col("cs.avg_amount"),
        F.col("cs.first_order_date"),
    )
)
