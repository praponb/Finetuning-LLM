from pyspark.sql import functions as F

remaining_keys = (
    df_transactions
    .select("account_id", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("amount") < 1600)
        .select("account_id", "merchant")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .rollup("activity_month", "account_id")
    .agg(
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.count("*").alias("num_rows"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .orderBy("fee_events")
)
