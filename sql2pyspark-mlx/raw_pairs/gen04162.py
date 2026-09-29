from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(~F.col("t.merchant").isin(["7-Eleven", "Grab"]))
    .groupBy(F.col("a.account_type"))
    .agg(F.count(F.col("t.id")).alias("transaction_count"))
)
