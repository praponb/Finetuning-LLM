from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("t.account_id") == 17)
    .groupBy(F.month(F.col("a.opened_date")).alias("opened_month"), F.col("a.status"))
    .agg(
        F.max(F.col("t.amount")).alias("max_amount"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
