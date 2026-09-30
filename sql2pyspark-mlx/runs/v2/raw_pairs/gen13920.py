from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 1600)
    .select("txn_time", "is_flagged")
    .orderBy("txn_time")
)
