from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") >= "2024-06-01") & ~F.col("merchant").isin(["Shell", "Amazon", "Lazada"]))
    .select(
        "merchant",
        F.when(F.col("txn_type").isin(["REFUND", "DEBIT"]), "Group A").otherwise("Other").alias("txn_type_group"),
        F.ceil("amount").alias("amount_whole"),
    )
    .orderBy("merchant")
)
