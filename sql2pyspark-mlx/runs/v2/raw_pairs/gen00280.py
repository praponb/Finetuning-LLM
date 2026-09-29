from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Starbucks", "7-Eleven"]) | ~F.col("is_flagged"))
    .select("id", "amount", F.upper("merchant").alias("merchant_upper"))
)
