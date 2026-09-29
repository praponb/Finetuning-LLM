from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("accounts.balance") > 56000)
    .groupBy(F.col("customers.city"))
    .agg(
        F.sum(F.when(F.col("accounts.account_type") == "CREDIT", F.col("accounts.balance")).otherwise(0)).alias("credit_balance"),
    )
    .orderBy("credit_balance")
)
