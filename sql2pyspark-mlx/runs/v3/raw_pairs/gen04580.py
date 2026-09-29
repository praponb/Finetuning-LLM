from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 1800)
    .groupBy("merchant", F.to_date("txn_time").alias("txn_day"))
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
    .filter(F.col("unique_txn_types") < 4)
    .select("merchant", "txn_day", "unique_txn_types")
    .orderBy(F.col("txn_day").desc(), "merchant")
    .limit(50)
)
