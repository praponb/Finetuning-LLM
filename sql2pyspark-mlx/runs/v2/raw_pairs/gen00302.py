from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.opened_date").between("2025-04-01", "2025-05-16"))
    .groupBy(F.col("a.id"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"),
    )
)
