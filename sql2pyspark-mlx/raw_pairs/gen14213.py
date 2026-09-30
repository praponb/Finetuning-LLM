from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "is_flagged",
        "txn_type",
        F.round((F.col("amount") * 0.9), 2).alias("amount_scaled"),
        (F.datediff(F.current_date(), "txn_time") / 365).alias("years_since_txn_time"),
    )
)
