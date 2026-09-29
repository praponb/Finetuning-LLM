from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.sum(F.when(F.col("t.merchant") == "7-Eleven", F.col("t.amount")).otherwise(0)).alias("7_eleven_amount"),
    )
    .orderBy("account_type", "7_eleven_amount")
)
