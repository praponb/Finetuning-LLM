from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        F.col("amount").cast("int").alias("amount_int"),
        F.upper("merchant").alias("merchant_upper"),
    )
    .orderBy(F.col("amount_int").desc())
)
