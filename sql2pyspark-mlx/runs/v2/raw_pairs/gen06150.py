from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((F.col("merchant") == "Lazada") & (F.col("amount") > 3500)))
    .groupBy("is_flagged")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
    .orderBy(F.col("pct_shell").desc(), "is_flagged")
    .limit(1)
)
