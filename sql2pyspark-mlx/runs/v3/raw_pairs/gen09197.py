from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.opened_date") < "2023-06-01")
    .groupBy(F.col("a.status"))
    .agg(
        F.sort_array(F.collect_set(F.col("t.txn_type"))).alias("txn_type_list"),
        F.sum(F.when(F.col("t.txn_type") == "FEE", F.col("t.amount")).otherwise(0)).alias("fee_amount"),
    )
    .orderBy("status")
    .limit(10)
)
