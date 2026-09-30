from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Amazon") & (F.col("amount") > 1700))
    .select(
        "txn_type",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.substring("txn_type", 1, 2).alias("txn_type_prefix"),
    )
    .orderBy("txn_type_prefix", "is_flagged_flag")
)
