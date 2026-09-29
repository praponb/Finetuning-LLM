from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "order_id",
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
)
