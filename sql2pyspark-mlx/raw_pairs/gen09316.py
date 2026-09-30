from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(F.col("a.status"))
    .agg(
        F.min(F.col("t.amount")).alias("min_amount"),
        F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"),
    )
    .orderBy("min_amount")
)
