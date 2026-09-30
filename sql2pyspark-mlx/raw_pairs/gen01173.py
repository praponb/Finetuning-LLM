from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
        F.count("*").alias("order_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("cs.pct_returned"),
        F.col("cs.order_count"),
    )
    .orderBy(F.col("pct_returned").desc(), F.col("order_count").desc())
)
