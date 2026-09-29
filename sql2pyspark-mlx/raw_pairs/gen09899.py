from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.month("end_date").isin([8, 10, 12]))
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("cs.num_subscriptions"),
        F.col("cs.cnt"),
    )
    .orderBy("email")
)
