from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(F.avg(F.col("t.amount")).alias("avg_amount"))
    .orderBy("opened_month")
)
