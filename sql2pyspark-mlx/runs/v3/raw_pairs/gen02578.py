from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("order_date") <= "2025-02-01")
    .groupBy("customer_id")
    .agg(
        F.sum("discount").alias("total_discount"),
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.total_discount") >= 2.3)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.is_active"),
        F.col("C.total_discount"),
        F.col("C.web_count"),
    )
    .orderBy(F.col("web_count").desc(), "id")
    .limit(10)
)
