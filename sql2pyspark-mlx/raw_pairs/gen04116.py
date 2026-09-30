from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & ((F.col("txn_time") >= "2023-11-18") | (F.to_date("txn_time") >= "2025-06-25")))
    .select("id", "amount", "merchant")
)
