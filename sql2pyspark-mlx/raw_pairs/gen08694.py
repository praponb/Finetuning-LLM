from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("accounts.balance").between(52000, 78000))
    .groupBy(F.col("customers.segment"))
    .agg(
        F.count("*").alias("account_count"),
        (F.max(F.col("accounts.balance")) - F.min(F.col("accounts.balance"))).alias("balance_range"),
    )
    .orderBy(F.col("segment").desc(), "balance_range")
)
