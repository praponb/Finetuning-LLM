from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "left")
    .filter(F.col("transactions.is_flagged") | F.col("accounts.account_type").isin(["CHECKING", "LOAN", "CREDIT"]))
    .select(
        F.col("transactions.id"),
        F.col("transactions.amount"),
        F.col("transactions.account_id"),
        F.col("transactions.merchant"),
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("accounts.balance"),
    )
    .orderBy(F.col("balance").desc())
)
