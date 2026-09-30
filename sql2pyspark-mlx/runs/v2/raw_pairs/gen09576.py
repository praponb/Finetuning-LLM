from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "CREDIT"]))
    .select("id", "merchant", F.length("txn_type").alias("txn_type_length"))
)
