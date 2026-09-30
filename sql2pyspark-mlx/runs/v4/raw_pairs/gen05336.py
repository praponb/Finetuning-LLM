from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .filter(F.col("A.row_count") >= 3)
    .select(F.col("a.account_type"), F.col("A.min_amount"), F.col("A.row_count"))
)
