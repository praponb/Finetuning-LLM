from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1190, 1310))
    .groupBy("method")
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
    .filter(F.col("pct_refunded") >= 43)
    .orderBy("method")
)
