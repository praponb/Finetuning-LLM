from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 840)
    .select(
        "status",
        "amount",
        F.date_trunc("MONTH", "paid_at").alias("paid_at_period"),
        F.concat_ws(",", "method", "status").alias("method_status_key"),
    )
    .orderBy("status")
)
