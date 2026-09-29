from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & ~F.col("is_flagged"))
    .select("id", "merchant", "txn_time", "account_id")
)
