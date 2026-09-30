from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "txn_time",
        F.when(F.col("amount") >= 3300, "High").when(F.col("amount") >= 1000, "Medium").otherwise("Low").alias("amount_band"),
    )
    .orderBy("amount_band")
)
