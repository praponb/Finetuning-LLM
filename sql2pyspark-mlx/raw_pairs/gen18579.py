from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_type") == "CREDIT") & (F.col("amount") >= 3900))
    .withColumn("merchant_txn_type_key", F.concat_ws("-", "merchant", "txn_type"))
)

summary = (
    base
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    summary
    .filter(F.col("max_amount") >= 1800)
    .select("txn_month", "max_amount")
)
