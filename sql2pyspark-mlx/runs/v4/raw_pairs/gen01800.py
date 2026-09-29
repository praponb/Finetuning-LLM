from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(10, 18))
    .select(
        "merchant",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)
