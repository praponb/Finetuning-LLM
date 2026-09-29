from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.avg_discount") >= 0.38)
    .select(
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("t.avg_discount"),
        F.col("t.last_order_date"),
    )
)
