from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("account_id") == 31))
    .select(
        "id",
        "txn_type",
        "amount",
        F.date_trunc("MONTH", "txn_time").alias("txn_time_period"),
        F.trim("merchant").alias("clean_merchant"),
    )
    .orderBy(F.col("id").desc(), "txn_type")
    .limit(3)
)
