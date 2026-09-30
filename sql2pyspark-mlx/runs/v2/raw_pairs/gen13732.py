from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Amazon")
    .select("id", "amount")
    .orderBy(F.col("id").desc())
)
