from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.col("a.status"), F.col("a.customer_id"))
    .agg(
        F.sum(F.when(F.col("t.merchant") == "Shell", F.col("t.amount")).otherwise(0)).alias("shell_amount"),
        F.count(F.when(F.col("t.merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
    )
    .orderBy(F.col("customer_id").desc())
)
