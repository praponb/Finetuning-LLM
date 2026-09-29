from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 4000)
    .select(
        "id",
        "amount",
        "is_flagged",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
    )
)
