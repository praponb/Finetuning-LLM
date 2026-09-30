from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2024-06-04") & (F.col("amount") < 2000))
    .groupBy("merchant")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
