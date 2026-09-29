from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
    .filter(F.col("pct_credit") < 49)
    .orderBy("pct_credit")
)
