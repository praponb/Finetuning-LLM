from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_accounts"),
        F.count("balance").alias("balance_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.num_accounts") > 8)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.segment"),
        F.col("t.num_accounts"),
        F.col("t.balance_count"),
    )
    .orderBy("num_accounts", "segment", "id")
    .limit(5)
)
