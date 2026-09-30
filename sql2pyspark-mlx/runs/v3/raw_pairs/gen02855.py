from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "merchant",
        F.when(F.col("merchant") == "Starbucks", 1).otherwise(0).alias("is_starbucks"),
    )
    .orderBy(F.col("merchant").desc())
)
