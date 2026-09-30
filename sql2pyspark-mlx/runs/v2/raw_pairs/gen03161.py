from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "amount",
        "order_id",
        F.col("paid_at").cast("date").alias("paid_at_day"),
    )
    .orderBy("paid_at_day", "id")
    .limit(5)
)
