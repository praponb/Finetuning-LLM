from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2024-07-20") & F.col("merchant").isin(["Starbucks", "Shell"]) & F.col("account_id").isin([1, 8, 29, 39]))
    .groupBy("txn_type", F.month("txn_time").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") > 14000)
)
