from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1700, 3800))
    .select(
        "id",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
    .orderBy("merchant", F.col("is_flagged_flag").desc(), "id")
    .limit(10)
)
