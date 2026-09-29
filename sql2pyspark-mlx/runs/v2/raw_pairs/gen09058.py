from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id").isin([10, 16, 32]) & F.hour("txn_time").between(11, 14)) | (F.col("txn_type") == "DEBIT"))
    .select("is_flagged", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
    .orderBy("amount_filled")
)
