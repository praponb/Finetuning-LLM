from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Lazada"]) & (F.col("amount") < 3400) & F.col("account_id").isin([15, 16, 40]))
    .select("txn_type", "is_flagged", "merchant")
)
