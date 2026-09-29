from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.year(F.col("a.opened_date")) >= 2023)
    .groupBy(F.col("c.segment"))
    .agg(
        F.sort_array(F.collect_set(F.col("t.merchant"))).alias("merchant_list"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 51)
    .orderBy(F.col("avg_amount").desc(), "segment")
)
