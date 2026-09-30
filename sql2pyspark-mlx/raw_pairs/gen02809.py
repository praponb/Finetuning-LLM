from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.balance_range") >= 55)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("cs.min_opened_date"),
        F.col("cs.balance_range"),
    )
)
