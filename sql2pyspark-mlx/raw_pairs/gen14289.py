from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.hour("paid_at").alias("paid_at_hour"),
    )
)
