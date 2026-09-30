from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.total_count") > 9)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("t.merchant_list"),
        F.col("t.total_count"),
    )
)
