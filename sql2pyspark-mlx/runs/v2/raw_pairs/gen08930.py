from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2023-09-01") & (F.col("txn_time") < "2023-07-31"))
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "merchant",
        F.floor("amount").alias("amount_whole"),
    )
)
