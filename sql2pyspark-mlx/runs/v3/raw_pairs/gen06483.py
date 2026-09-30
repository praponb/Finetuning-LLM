from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        F.when(F.col("amount") >= 3600, "High").when(F.col("amount") >= 1000, "Medium").otherwise("Low").alias("amount_band"),
    )
)
