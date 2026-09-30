from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("customer_id").isin([3, 5, 28]))
    .groupBy("customer_id")
    .agg(
        F.min("amount").alias("min_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .filter(F.col("customer_stats.amount_range") > 50)
    .select(
        F.col("customers.id"),
        F.col("customers.email"),
        F.col("customers.segment"),
        F.col("customer_stats.min_amount"),
        F.col("customer_stats.amount_range"),
    )
    .orderBy("segment")
)
