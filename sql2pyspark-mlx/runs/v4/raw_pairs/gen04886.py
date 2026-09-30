from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((~F.col("is_flagged") & F.hour("txn_time").between(12, 19)) | (F.to_date("txn_time") < "2023-04-01")))
    .select(
        "id",
        "account_id",
        "txn_time",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy(F.col("is_flagged_flag").desc(), F.col("account_id").desc())
)
