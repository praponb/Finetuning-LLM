from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Shell"]) & ((F.col("txn_type") != "REFUND") | F.col("merchant").isin(["Starbucks", "Amazon", "Grab"])))
    .select(
        "merchant",
        F.when(F.col("amount") >= 3500, "High").when(F.col("amount") >= 1000, "Medium").otherwise("Low").alias("amount_band"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("is_flagged_flag").desc())
)
