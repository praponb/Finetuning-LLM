from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") >= 2900) & F.col("merchant").isin(["Amazon", "7-Eleven"]))
    .select(
        "merchant",
        "amount",
        F.when(F.col("amount") >= 4400, "High").when(F.col("amount") >= 700, "Medium").otherwise("Low").alias("amount_band"),
    )
    .orderBy("amount_band")
)
