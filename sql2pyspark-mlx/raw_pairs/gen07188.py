from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2023-10-18")
    .select(
        "id",
        "txn_type",
        F.concat("txn_type", F.lit(" "), "merchant").alias("txn_type_merchant"),
    )
)
