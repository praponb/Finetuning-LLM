from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.amount_count") >= 10)
    .select(
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("at.amount_count"),
        F.col("at.total_amount"),
    )
    .orderBy(F.col("total_amount").desc())
)
