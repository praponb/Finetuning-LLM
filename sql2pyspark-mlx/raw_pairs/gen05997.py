from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("a.customer_id").isNull())
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.count(F.col("t.amount")).alias("amount_count"),
        F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"),
    )
    .filter(F.col("transaction_count") > 4)
    .orderBy("status", F.col("transaction_count").desc())
)
