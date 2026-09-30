from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("id", "account_id", "is_flagged", F.ceil("amount").alias("amount_whole"))
)
