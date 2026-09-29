from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        F.when(F.col("amount") >= 3600, "High").when(F.col("amount") >= 1100, "Medium").otherwise("Low").alias("amount_level"),
        F.when(F.col("amount") >= 3200, "High").when(F.col("amount") >= 1400, "Medium").otherwise("Low").alias("amount_level_2"),
    )
    .orderBy(F.col("merchant").desc())
)
