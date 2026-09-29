from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Starbucks", "Shell", "7-Eleven"]))
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
    )
    .filter(F.col("starbucks_count") < 29)
    .drop("starbucks_count")
    .orderBy("unique_accounts", "txn_month")
    .limit(50)
)
