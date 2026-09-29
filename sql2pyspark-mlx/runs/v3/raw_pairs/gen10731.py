from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("merchant").isin(["Starbucks", "Lazada"]))
    .withColumn("txn_type_merchant_key", F.concat_ws("-", "txn_type", "merchant"))
    .orderBy(F.col("account_id").desc(), "id")
    .limit(5)
)
