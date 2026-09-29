from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Grab") | F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
    )
)
