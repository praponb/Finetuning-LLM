from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") > 2800) & (F.col("amount") < 2900) & F.col("account_id").isin([5, 10]))
    .select(
        "id",
        "txn_time",
        F.when(F.col("txn_type") == "REFUND", 1).otherwise(0).alias("is_refund"),
        F.substring("txn_type", 1, 3).alias("txn_type_prefix"),
    )
)
