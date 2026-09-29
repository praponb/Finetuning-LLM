from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count_if(F.col("txn_type") == "CREDIT").alias("credit_events"),
        F.max("amount").alias("max_amount"),
        F.min("txn_time").alias("first_txn_time"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
    )
    .orderBy(F.col("max_amount").desc_nulls_last())
)
