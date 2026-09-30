from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "method",
        F.when(F.col("method") == "cash", 1).otherwise(0).alias("is_cash"),
    )
)
