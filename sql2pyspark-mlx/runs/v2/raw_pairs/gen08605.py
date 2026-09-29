from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([4, 15, 37]))
    .select(
        "id",
        "status",
        "order_id",
        "amount",
        F.floor("amount").alias("amount_whole"),
    )
)
