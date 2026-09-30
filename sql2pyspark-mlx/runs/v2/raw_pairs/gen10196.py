from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CHECKING", "SAVINGS"]))
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("abc"), F.col("c.id") == F.col("abc.customer_id"), "left")
    .filter(F.col("abc.pct_savings") > 51)
    .select(F.col("c.id"), F.col("c.email"), F.col("abc.pct_savings"))
    .orderBy("pct_savings")
)
