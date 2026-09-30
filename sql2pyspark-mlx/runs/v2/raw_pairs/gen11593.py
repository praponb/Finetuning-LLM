from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("discount").alias("max_discount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.avg_amount") >= 1310)
    .select(
        F.col("c.name"),
        F.col("c.city"),
        F.col("C.avg_amount"),
        F.col("C.max_discount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
