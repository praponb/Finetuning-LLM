from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("customer_id").isNotNull()).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .select(F.col("t.id"), F.col("t.merchant"), F.col("t.amount"))
    .orderBy(F.col("amount").desc(), F.col("merchant").desc())
)
