from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "paid_at",
        "order_id",
        "method",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
        F.round((F.col("amount") * 1.07), 2).alias("amount_scaled"),
    )
)
