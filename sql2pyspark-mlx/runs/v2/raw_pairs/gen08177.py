from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("txn_type_merchant_key", F.concat_ws("-", "txn_type", "merchant"))
)
