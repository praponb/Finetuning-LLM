from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("opened_date") > "2025-01-01")
    .groupBy("customer_id")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0)).alias("loan_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(
        F.col("c.credit_limit"),
        F.col("c.is_active"),
        F.col("C.total_balance"),
        F.col("C.loan_count"),
    )
)
