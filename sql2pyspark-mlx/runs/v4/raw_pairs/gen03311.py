from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .filter(F.col("at.min_amount") > 2000)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("at.min_amount"),
    )
    .orderBy(F.col("min_amount").desc(), "customer_id")
)
