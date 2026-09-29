from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "is_flagged",
        F.length("txn_type").alias("txn_type_length"),
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
    .orderBy(F.col("id").desc())
)
