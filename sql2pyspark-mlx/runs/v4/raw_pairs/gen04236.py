from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-06-20 08:00:00").cast("timestamp")) & (F.to_date("txn_time") < "2024-01-27"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.lower("txn_type").alias("txn_type_lower"),
    )
)

result = (
    cleaned
    .groupBy("txn_type")
    .pivot("merchant", ["7-Eleven", "Lazada", "Grab"])
    .agg(F.sum("amount").alias("total"))
    .select(
        "txn_type",
        F.col("7-Eleven").alias("v_7_eleven"),
        F.col("Lazada").alias("lazada"),
        F.col("Grab").alias("grab"),
    )
)
