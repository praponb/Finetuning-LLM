from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "txn_type",
        F.when(F.col("merchant").isin(["Grab", "Lazada"]), "priority").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") > 22000)
    .select("txn_type", "merchant_group", "min_txn_time", "total_amount")
)
