from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2023-05-29") & F.col("merchant").isNotNull())
    .select(
        "id",
        "amount",
        "is_flagged",
        "txn_type",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("id").desc())
)
