from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.month("opened_date").isin([7, 8, 10])).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .select(F.col("t.id"), F.col("t.account_id"), F.col("t.merchant"))
    .orderBy("id")
)
