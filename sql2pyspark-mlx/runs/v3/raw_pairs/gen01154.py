from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(
        F.col("a.account_type"),
        F.month(F.col("a.opened_date")).alias("opened_month"),
    )
    .agg(F.min(F.col("t.amount")).alias("min_amount"))
    .orderBy("account_type")
)
