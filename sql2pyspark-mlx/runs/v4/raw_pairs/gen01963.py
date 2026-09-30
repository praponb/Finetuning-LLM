from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(3700, 3800))
    .select(
        "id",
        "amount",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)
