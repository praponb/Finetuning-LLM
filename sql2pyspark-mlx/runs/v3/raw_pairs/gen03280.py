from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .select(
        "id",
        "merchant",
        "is_flagged",
        F.when(F.col("amount") >= 4400, "High").when(F.col("amount") >= 1400, "Medium").otherwise("Low").alias("amount_band"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("amount_band", "is_flagged_flag")
)
