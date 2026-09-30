from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.sum("amount").alias("total_amount"),
        F.max("txn_time").alias("last_seen"),
    )
    .orderBy(F.col("avg_amount").desc_nulls_first())
)
