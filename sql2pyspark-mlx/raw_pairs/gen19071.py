from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_semi")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
    )
    .orderBy("opened_date", F.col("id").desc())
)
