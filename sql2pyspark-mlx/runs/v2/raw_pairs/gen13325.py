from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("merchant", "account_id", "txn_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") > 1)
    .orderBy("merchant_values", "merchant", "account_id", "txn_type")
    .limit(5)
)
