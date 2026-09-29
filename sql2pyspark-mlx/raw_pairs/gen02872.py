from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.customer_id").isNotNull())
    .groupBy(F.col("c.country"))
    .agg(
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.merchant")))).alias("merchant_values"),
    )
    .orderBy("country")
    .limit(20)
)
