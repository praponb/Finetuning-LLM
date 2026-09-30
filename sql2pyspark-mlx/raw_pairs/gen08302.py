from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-07-21"))) & F.col("txn_type").isin(["CREDIT", "FEE"]))
    .withColumn("is_lazada", F.when(F.col("merchant") == "Lazada", 1).otherwise(0))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

result = (
    filtered
    .groupBy("account_id", "txn_type")
    .pivot("merchant", ["Grab", "7-Eleven", "Shell"])
    .agg(F.sum("amount"))
    .select(
        "account_id",
        "txn_type",
        F.col("Grab").alias("grab"),
        F.col("7-Eleven").alias("v_7_eleven"),
        F.col("Shell").alias("shell"),
    )
)
