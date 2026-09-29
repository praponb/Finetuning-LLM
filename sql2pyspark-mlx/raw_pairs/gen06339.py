from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy(
        "txn_type",
        F.hour("txn_time").alias("txn_hour"),
        F.to_date("txn_time").alias("txn_day"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_debit"),
        F.count("amount").alias("amount_count"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0))) / F.count("*")), 1).alias("pct_starbucks"),
    )
)
