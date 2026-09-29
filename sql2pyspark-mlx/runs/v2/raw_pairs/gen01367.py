from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "inner")
    .select(F.col("c.credit_limit"), F.col("abc.closed_balance"))
    .orderBy(F.col("credit_limit").desc())
)
