from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
    )
)
