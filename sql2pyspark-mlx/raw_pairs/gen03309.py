from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Starbucks", "Grab"]))
    .groupBy("txn_type")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
    )
    .filter(F.col("fee_amount") >= 68)
    .select("txn_type", "stddev_amount", "fee_amount")
    .orderBy("txn_type")
)
