from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
        "merchant",
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.max("txn_time").alias("last_txn_time"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy(F.col("num_txn_types").desc())
)
