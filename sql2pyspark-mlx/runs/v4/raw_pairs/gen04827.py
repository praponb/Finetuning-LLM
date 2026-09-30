from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-08-04")
    .select(
        "id",
        "paid_at",
        "order_id",
        "amount",
        F.round((F.col("amount") * 1.1), 2).alias("amount_scaled"),
    )
)
