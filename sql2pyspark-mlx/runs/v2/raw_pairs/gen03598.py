from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.total_amount") > 19000)
    .select(F.col("a.customer_id"), F.col("a.opened_date"), F.col("t.total_amount"))
    .orderBy(F.col("total_amount").desc(), F.col("customer_id").desc())
)
