from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 2600)
    .select("id", "amount")
)
