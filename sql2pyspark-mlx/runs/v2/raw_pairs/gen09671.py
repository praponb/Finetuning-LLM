from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.total_count") > 7)
    .select(
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("cs.total_count"),
        F.col("cs.avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
