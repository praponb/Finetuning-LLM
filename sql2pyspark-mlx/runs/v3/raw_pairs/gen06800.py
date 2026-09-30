from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-04-01")
    .groupBy("txn_type")
    .agg(
        F.count("*").alias("num_transactions"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
