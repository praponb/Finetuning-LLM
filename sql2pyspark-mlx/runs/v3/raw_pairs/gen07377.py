from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .groupBy(F.col("a.customer_id"))
    .agg(F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"))
    .orderBy(F.col("customer_id").desc())
)
