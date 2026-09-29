from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2024-01-29").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.concat("merchant", F.lit("/"), "txn_type").alias("merchant_txn_type"),
    )
)

filtered = (
    base
    .filter((F.col("txn_time") >= F.lit("2023-11-20 19:00:00").cast("timestamp")) & F.col("is_flagged_flag").isNotNull())
    .select(
        "id",
        "account_id",
        "is_flagged",
        "is_flagged_flag",
        F.to_date("txn_time").alias("txn_time_date"),
    )
)

filtered_transactions = (
    filtered
    .filter(F.col("is_flagged_flag") <= 53)
    .select(
        "id",
        "is_flagged",
        "is_flagged_flag",
        "txn_time_date",
        F.date_format("txn_time_date", "yyyy-MM-dd").alias("txn_time_date_label"),
    )
)

result = (
    filtered_transactions
    .select("id", "is_flagged", "txn_time_date", "txn_time_date_label")
    .orderBy(F.col("id").desc_nulls_first())
)
