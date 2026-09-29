from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("a.customer_id") == 24)
    .groupBy(F.col("c.id"))
    .agg(
        F.count(F.col("a.id")).alias("account_count"),
        F.count(F.col("a.balance")).alias("balance_count"),
    )
)
