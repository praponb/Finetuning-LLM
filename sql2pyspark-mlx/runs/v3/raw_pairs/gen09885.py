from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.date_format(F.col("a.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("t.merchant") == "Lazada", F.col("t.amount")).otherwise(0)).alias("lazada_amount"),
        F.count(F.col("t.amount")).alias("amount_count"),
    )
)
