from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Starbucks", "Lazada", "Grab"]) & (F.col("merchant") == "Grab"))
    .select(
        "merchant",
        F.floor("amount").alias("amount_whole"),
        F.lower("merchant").alias("merchant_lower"),
    )
    .orderBy(F.col("merchant_lower").desc())
)
