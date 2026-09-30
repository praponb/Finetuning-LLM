from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("amount").between(590, 910))
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "left")
    .select(
        F.col("customers.age"),
        F.col("customer_summary.avg_discount"),
        F.col("customer_summary.avg_amount"),
    )
)
