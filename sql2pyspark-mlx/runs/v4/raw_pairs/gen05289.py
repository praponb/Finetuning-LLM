from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("amount") >= 4000))
    .select("id", "txn_type", "is_flagged")
)
