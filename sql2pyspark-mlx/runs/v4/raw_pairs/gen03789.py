from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-11-20 23:00:00").cast("timestamp")) & F.col("merchant").isNotNull() & (F.col("amount") > 2100))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.lower("txn_type").alias("txn_type_lower"),
    )
)

result = (
    valid
    .groupBy("account_id")
    .pivot("merchant", ["Grab", "Shell", "7-Eleven"])
    .agg(F.max("amount"))
    .select(
        "account_id",
        F.col("Grab").alias("grab"),
        F.col("Shell").alias("shell"),
        F.col("7-Eleven").alias("v_7_eleven"),
    )
)
