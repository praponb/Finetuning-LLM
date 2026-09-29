from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        F.when(F.col("status") == "FAILED", 1).otherwise(0).alias("is_failed"),
    )
)
