from pyspark.sql import functions as F

src = df_transactions.filter(F.col("account_id") == 22)

summary = (
    src
    .groupBy("merchant", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
)

result = (
    summary
    .filter(F.col("unique_merchants") > 7)
    .orderBy(F.col("unique_merchants").desc(), F.col("debit_count").desc())
)
