from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1400, 3500))
    .select(
        "id",
        "merchant",
        "amount",
        "account_id",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
