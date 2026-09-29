from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("customer_id").isin([4, 5, 32]))
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.savings_count") > 5)
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
        F.col("t.credit_count"),
        F.col("t.savings_count"),
    )
)
