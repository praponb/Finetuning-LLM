from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() | F.hour("paid_at").between(12, 18))
    .select(
        "id",
        "method",
        F.round("amount", 0).alias("amount_rounded"),
        F.upper("status").alias("status_upper"),
    )
)
