from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 3000)
    .select(
        "id",
        "amount",
        "is_flagged",
        "account_id",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
