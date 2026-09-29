from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([20, 32]) | (F.col("amount") <= 3300))
    .select("id", "merchant", "amount")
)
