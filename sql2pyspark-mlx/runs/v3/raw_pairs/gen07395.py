from pyspark.sql import functions as F

base = df_transactions.filter(F.col("account_id").isin([1, 31]))

agg = (
    base
    .groupBy("is_flagged")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
    )
)

result = (
    agg
    .filter(F.col("pct_fee") > 63)
    .select("is_flagged", "max_amount", "pct_fee")
    .orderBy(F.col("pct_fee").desc(), F.col("max_amount").desc())
)
