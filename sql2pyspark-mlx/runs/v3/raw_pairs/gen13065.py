from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("is_flagged") | (F.col("txn_time") < "2025-08-11")))
    .select("id", "is_flagged", "account_id")
    .orderBy("account_id")
)
