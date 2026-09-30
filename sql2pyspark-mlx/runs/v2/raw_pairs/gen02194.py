from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.customer_id").isNull())
    .groupBy(F.col("a.status"))
    .agg(F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"))
    .orderBy("avg_amount")
)
