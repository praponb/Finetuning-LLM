from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([3, 17, 29]))
    .select("account_id")
)
