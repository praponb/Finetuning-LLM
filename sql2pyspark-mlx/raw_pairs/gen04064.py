from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.credit_limit"),
        F.col("cs.first_opened_date"),
        F.col("cs.total_balance"),
    )
    .orderBy("credit_limit")
)
