from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "amount",
        "paid_at",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
        F.hour("paid_at").alias("paid_at_hour"),
    )
)
