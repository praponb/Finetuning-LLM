from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "FEE"]))
    .select(
        "id",
        "amount",
        "merchant",
        "txn_time",
        F.round((F.col("amount") * 100), 2).alias("adjusted_amount"),
    )
)
