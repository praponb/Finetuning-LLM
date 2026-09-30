from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 650) & (F.col("amount") <= 560))
    .select(
        "id",
        "status",
        "method",
        "amount",
        F.when(F.col("status") == "FAILED", 1).otherwise(0).alias("is_failed"),
    )
)
