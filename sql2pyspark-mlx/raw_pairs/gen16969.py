from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
    )
    .filter(F.col("amount_range") < 34)
    .orderBy("amount_range")
)
