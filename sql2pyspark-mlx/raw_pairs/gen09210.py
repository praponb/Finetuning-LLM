from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("txn_time") >= "2025-03-01") & ~F.col("is_flagged"))
    .select("id", "merchant")
)
