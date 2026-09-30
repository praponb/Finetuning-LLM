from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([5, 30, 35]))
    .select(
        "id",
        "method",
        "status",
        "order_id",
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
)
