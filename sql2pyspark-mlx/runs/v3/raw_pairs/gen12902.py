from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.length(F.col("c.name")) > 11)
    .groupBy(F.col("a.status"))
    .agg(F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"))
    .orderBy(F.col("stddev_amount").desc())
)
