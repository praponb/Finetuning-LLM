from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 26)
    .orderBy("account_id", "id")
    .limit(5)
)
