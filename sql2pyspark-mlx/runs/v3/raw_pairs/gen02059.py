from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-07-14")
    .groupBy("merchant")
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.countDistinct("amount").alias("unique_merchants"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
    )
    .filter(F.col("pct_amazon") < 79)
    .orderBy("unique_merchants")
)
