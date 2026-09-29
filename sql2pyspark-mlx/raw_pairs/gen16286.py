from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance") <= 17000).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.col("t.amount") < 3900)
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
    )
    .orderBy(F.col("amount").desc(), "id")
    .limit(1)
)
