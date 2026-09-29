from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "7-Eleven") & ~F.col("is_flagged"))
    .orderBy(F.col("id").desc())
)
