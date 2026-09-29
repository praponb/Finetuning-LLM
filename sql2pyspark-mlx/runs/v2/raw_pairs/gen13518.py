from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        F.weekofyear("txn_time").alias("txn_time_weekofyear"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
