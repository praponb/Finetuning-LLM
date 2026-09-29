from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("account_id").isin([28, 32, 38, 40])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.account_type").isin(["CHECKING", "LOAN", "CREDIT"]))
    .select(F.col("a.id"), F.col("a.balance"), F.col("a.opened_date"))
)
