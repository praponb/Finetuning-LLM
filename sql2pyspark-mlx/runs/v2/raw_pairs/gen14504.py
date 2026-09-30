from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.when(F.col("t.merchant") == "Lazada", 1)).alias("lazada_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.txn_type")))).alias("txn_type_values"),
    )
    .orderBy("txn_type_values", "status")
    .limit(1)
)
