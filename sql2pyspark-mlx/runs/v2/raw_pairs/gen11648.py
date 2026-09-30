from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(F.col("a.status"))
    .agg(
        F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"),
        F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"),
    )
    .orderBy("status")
)
