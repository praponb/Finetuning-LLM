from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "merchant",
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        "txn_type",
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount_2"),
        F.count("*").alias("cnt"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("amount_per_refund"),
    )
    .orderBy(F.col("avg_amount").desc())
)
