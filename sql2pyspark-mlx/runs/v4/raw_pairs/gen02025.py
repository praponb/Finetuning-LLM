from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 2100)
    .select("id", "merchant")
)
