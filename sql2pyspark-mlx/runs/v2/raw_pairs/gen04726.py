from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.col("amount").between(3000, 3300) & (F.col("amount") < 3000))
)

agg = (
    src
    .groupBy("txn_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    agg
    .filter(F.col("max_amount") > 2900)
    .orderBy("pct_fee", "txn_type")
    .limit(20)
)
