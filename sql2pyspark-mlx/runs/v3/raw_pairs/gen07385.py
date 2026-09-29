from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("t.amount") <= 2500)
    .groupBy(F.col("a.customer_id"))
    .agg(
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
        F.count("*").alias("cnt"),
    )
    .orderBy("avg_amount")
)
