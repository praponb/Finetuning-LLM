from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
    )
    .agg(
        F.min("txn_time").alias("started_at"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("amount_per_refund"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
)
