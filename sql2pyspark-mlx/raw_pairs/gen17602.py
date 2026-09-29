from pyspark.sql import functions as F

result = (
    df_transactions
    .select("merchant", "amount")
    .orderBy(F.col("merchant").desc(), "amount")
)
