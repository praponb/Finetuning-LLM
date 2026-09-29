from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 29, 34]) & F.col("account_id").isin([4, 6, 31, 37]))
    .select("id", "amount")
)
