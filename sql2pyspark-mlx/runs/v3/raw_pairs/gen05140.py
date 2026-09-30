from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 1600) & (F.col("txn_time") < "2024-08-01"))
    .select(
        "id",
        "txn_time",
        "txn_type",
        F.upper("txn_type").alias("txn_type_upper"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)
