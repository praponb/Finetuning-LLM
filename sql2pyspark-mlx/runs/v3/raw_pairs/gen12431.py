from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "status",
        "paid_at",
        "method",
        F.concat_ws("/", "method", "status").alias("method_status_key"),
    )
    .orderBy("method_status_key")
)
