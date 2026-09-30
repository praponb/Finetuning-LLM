from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(~F.col("transactions.is_flagged"))
    .groupBy(F.col("accounts.account_type"))
    .agg(
        (F.max(F.col("transactions.amount")) - F.min(F.col("transactions.amount"))).alias("amount_range"),
    )
    .orderBy("account_type", F.col("amount_range").desc())
)
