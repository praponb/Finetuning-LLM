from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") >= 2800)
    .select("id", "merchant", F.substring("txn_type", 1, 4).alias("txn_type_prefix"))
)
