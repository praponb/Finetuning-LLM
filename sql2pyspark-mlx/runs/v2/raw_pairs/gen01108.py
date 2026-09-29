from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([16, 35]))
    .groupBy("txn_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
    )
    .orderBy(F.col("pct_lazada").desc())
)
