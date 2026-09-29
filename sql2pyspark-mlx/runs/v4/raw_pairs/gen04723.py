from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2024-08-01") & (F.col("account_id").isin([29, 34]) | (F.to_date("txn_time") < "2025-05-01")))
    .select("id", "txn_type", "txn_time")
    .orderBy(F.col("txn_time").desc())
)
