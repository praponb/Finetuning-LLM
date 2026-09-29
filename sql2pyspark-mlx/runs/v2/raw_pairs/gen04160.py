from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["FEE", "CREDIT", "DEBIT"]) & F.col("is_flagged") & (F.col("txn_time") < "2024-09-05"))
    .select(
        "id",
        "txn_time",
        "is_flagged",
        F.dayofweek("txn_time").alias("txn_time_dayofweek"),
    )
)
