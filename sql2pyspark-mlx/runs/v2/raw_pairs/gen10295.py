from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .filter(F.hour(F.col("t.txn_time")).between(11, 14))
    .groupBy(F.col("a.status"), F.col("a.account_type"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.merchant")))).alias("merchant_values"),
    )
)
