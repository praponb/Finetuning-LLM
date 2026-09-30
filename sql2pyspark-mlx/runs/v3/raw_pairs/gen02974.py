from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amount_count") > 6)
    .select(
        F.col("accounts.id"),
        F.col("accounts.customer_id"),
        F.col("t.amount_count"),
    )
    .orderBy(F.col("id").desc())
)
