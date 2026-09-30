from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 3000)
    .select("id", "is_flagged")
    .orderBy("id")
    .limit(10)
)
