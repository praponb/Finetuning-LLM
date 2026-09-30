from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") != "Amazon")
    .select("amount")
    .orderBy("amount")
)
