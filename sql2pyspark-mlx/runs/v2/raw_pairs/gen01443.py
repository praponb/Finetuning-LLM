from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "order_id",
        "paid_at",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
    )
)
