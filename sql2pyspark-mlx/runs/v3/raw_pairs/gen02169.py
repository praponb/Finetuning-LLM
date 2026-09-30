from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~(F.col("amount").isNotNull() & F.col("account_id").isin([8, 23, 29])))
    .select(
        "txn_type",
        F.when(F.col("merchant").isin(["Amazon", "Shell"]), "Priority").otherwise("Other").alias("merchant_group"),
    )
    .orderBy("txn_type")
)
