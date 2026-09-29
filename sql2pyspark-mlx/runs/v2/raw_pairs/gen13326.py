from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.col("transactions.amount").between(3300, 3900) & (F.col("accounts.status") == "OPEN"))
    .select(
        F.col("transactions.id"),
        F.col("transactions.merchant"),
        F.col("accounts.balance"),
    )
    .orderBy(F.col("balance").desc(), "id")
    .limit(20)
)
