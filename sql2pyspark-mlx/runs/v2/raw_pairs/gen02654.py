from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("amount") < 1200)

transactions_stats = (
    valid_transactions
    .groupBy("txn_type", "merchant")
    .agg(
        F.min("amount").alias("min_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    transactions_stats
    .filter(F.col("min_amount") >= 2200)
    .select("txn_type", "merchant", "merchant_values")
    .orderBy("txn_type")
)
