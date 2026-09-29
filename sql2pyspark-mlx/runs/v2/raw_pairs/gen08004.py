from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type", F.substring("merchant", 1, 2).alias("merchant_prefix"))
    .orderBy("merchant_prefix")
)
