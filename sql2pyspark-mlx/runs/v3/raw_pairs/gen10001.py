from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("customer_id") == 12).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .select(F.col("t.id"), F.col("t.is_flagged"), F.col("t.amount"))
    .orderBy(F.col("amount").desc(), F.col("id").desc())
)
