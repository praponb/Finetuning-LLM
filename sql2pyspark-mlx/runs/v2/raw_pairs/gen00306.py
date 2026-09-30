from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(
        F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_month"),
        F.when(F.col("a.balance") >= 75000, "high").when(F.col("a.balance") >= 36000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"))
    .orderBy("opened_month")
)
