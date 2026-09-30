from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("amount") > 2900))
    .select(
        "txn_type",
        F.concat("merchant", F.lit(" - "), "txn_type").alias("merchant_txn_type"),
    )
    .orderBy("txn_type")
)
