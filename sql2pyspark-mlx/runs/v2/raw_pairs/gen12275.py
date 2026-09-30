from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "amount",
        "order_id",
        F.trim("method").alias("clean_method"),
    )
)
