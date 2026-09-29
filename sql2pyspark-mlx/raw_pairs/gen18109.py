from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.customer_id"))
    .agg(
        F.sum(F.when(F.col("t.merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
        F.count("*").alias("transaction_count"),
    )
    .orderBy("starbucks_count")
)
