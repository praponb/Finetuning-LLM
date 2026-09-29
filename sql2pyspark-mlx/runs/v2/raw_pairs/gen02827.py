from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.balance").between(19000, 61000) | F.col("a.account_type").isin(["CREDIT", "CHECKING"]))
    .select(F.col("t.account_id"), F.col("a.status"), F.col("a.opened_date"))
)
