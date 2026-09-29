from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "DEBIT", "CREDIT"]) & F.col("merchant").isNotNull())
    .groupBy("is_flagged")
    .agg(F.countDistinct("amount").alias("unique_merchants"))
    .filter(F.col("unique_merchants") > 2)
    .orderBy(F.col("unique_merchants").desc())
)
