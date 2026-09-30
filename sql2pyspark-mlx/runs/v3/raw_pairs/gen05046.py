from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("order_date") < "2023-07-01")
    .groupBy("customer_id")
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.email"), F.col("cs.avg_discount"), F.col("cs.total_count"))
    .orderBy(F.col("total_count").desc(), F.col("avg_discount").desc())
)
