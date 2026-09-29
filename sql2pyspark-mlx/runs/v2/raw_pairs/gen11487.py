from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 3300) & F.col("merchant").isin(["7-Eleven", "Lazada"]))
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "txn_time",
        F.dayofweek("txn_time").alias("txn_time_dayofweek"),
        F.weekofyear("txn_time").alias("txn_time_weekofyear"),
    )
)
