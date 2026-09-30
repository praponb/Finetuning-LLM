from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "merchant",
        "txn_type",
        F.date_trunc("YEAR", "txn_time").alias("txn_time_period"),
    )
    .orderBy("merchant")
)
