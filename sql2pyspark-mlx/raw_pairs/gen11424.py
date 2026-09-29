from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "amount")
    .orderBy(F.col("id").desc())
)
