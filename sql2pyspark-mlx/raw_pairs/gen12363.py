from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Amazon", "Lazada"])
    .agg(F.avg("amount").alias("value_sum"))
    .select(
        "txn_type",
        F.col("Amazon").alias("amazon"),
        F.col("Lazada").alias("lazada"),
    )
)
