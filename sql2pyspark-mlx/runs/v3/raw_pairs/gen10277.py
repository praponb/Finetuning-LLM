from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_time") < "2025-10-01")
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .filter(F.col("A.pct_amazon") > 59)
    .select(
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("A.merchant_values"),
        F.col("A.pct_amazon"),
    )
)
