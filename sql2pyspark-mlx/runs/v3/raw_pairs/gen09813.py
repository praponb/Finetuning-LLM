from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "method",
        "status",
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label"),
    )
)
