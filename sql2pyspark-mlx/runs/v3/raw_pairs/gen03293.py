from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("customer_id").isin([4, 39]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("A"), F.col("c.id") == F.col("A.customer_id"), "left")
    .select(F.col("c.credit_limit"), F.col("A.credit_balance"))
    .orderBy("credit_limit", F.col("credit_balance").desc())
)
