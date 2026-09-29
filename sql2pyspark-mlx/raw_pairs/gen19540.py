from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "is_flagged")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0))) / F.count("*")), 1).alias("pct_starbucks"),
    )
    .orderBy("txn_type", "is_flagged")
    .limit(3)
)
