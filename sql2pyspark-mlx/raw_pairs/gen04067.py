from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([15, 23]) & F.hour("txn_time").between(8, 15))
    .select(
        "id",
        "txn_time",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)
