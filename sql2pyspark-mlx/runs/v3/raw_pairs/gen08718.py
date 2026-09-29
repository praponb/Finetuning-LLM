from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(7, 13))
    .select(
        "id",
        "merchant",
        "txn_type",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
)
