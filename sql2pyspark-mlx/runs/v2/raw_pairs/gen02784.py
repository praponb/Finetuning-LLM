from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("txn_time") < "2024-07-17") & F.col("is_flagged")) | F.col("account_id").isin([13, 21]))
    .select(
        "id",
        "txn_time",
        "is_flagged",
        F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
    .orderBy("id")
)
