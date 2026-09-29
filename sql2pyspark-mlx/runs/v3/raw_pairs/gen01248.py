from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
    )
)

result = (
    df_customers.alias("customers")
    .join(accounts_by_customer.alias("accounts_by_customer"), F.col("customers.id") == F.col("accounts_by_customer.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.city"),
        F.col("customers.segment"),
        F.col("accounts_by_customer.frozen_balance"),
    )
    .orderBy(F.col("id").desc())
)
