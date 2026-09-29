from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.name"), F.col("abc.checking_count"))
)
