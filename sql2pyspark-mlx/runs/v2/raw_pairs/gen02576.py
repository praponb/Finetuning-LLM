from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 4)
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.hour("paid_at").alias("paid_at_hour"),
    )
    .orderBy(F.col("amount").desc())
)
