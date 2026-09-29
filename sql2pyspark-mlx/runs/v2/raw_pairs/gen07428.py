from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.year("txn_time").alias("txn_year"),
        F.when(F.col("amount") >= 4000, "high").when(F.col("amount") >= 800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
    .filter(F.col("debit_count") > 65)
    .orderBy(F.col("debit_count").desc(), "txn_year", "amount_bucket")
    .limit(50)
)
