from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "paypal")
    .select(
        "id",
        "paid_at",
        "method",
        "amount",
        F.length("method").alias("method_length"),
    )
)
