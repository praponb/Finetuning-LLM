from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .groupBy(
        F.when(F.col("a.balance") >= 72000, "high").when(F.col("a.balance") >= 29000, "mid").otherwise("low").alias("balance_bucket"),
        F.col("a.customer_id"),
    )
    .agg(F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"))
)
