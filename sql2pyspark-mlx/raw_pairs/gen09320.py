from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter((F.col("merchant") == "Grab") & F.col("account_id").isin([1, 4, 20, 33]))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

txn_type_summary = (
    filtered_transactions
    .groupBy("txn_type")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    txn_type_summary
    .select("txn_type", "stddev_amount")
    .orderBy("stddev_amount", "txn_type")
    .limit(3)
)
