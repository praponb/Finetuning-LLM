from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("merchant")
    .distinct()
    .orderBy("merchant")
    .limit(5)
)
