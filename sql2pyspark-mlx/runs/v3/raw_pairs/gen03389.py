from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"), "account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy(F.col("account_id").desc(), F.col("fee_count").desc(), "txn_month")
    .limit(5)
)
