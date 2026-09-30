from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("txn_type").isin(["FEE", "REFUND"]), "core").otherwise("other").alias("txn_type_group"),
        "txn_type",
        "merchant",
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
    )
    .filter(F.col("pct_amazon") > 57)
    .orderBy("txn_type_group")
)
