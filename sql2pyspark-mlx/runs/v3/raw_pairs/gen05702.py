from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 2000)
    .select("txn_type", "merchant")
    .distinct()
)
