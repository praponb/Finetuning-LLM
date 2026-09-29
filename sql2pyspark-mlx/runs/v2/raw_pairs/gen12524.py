from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.account_type").isin(["LOAN", "CREDIT", "CHECKING"]))
    .select(F.col("a.id"), F.col("a.opened_date"))
    .orderBy(F.col("opened_date").desc(), F.col("id").desc())
)
