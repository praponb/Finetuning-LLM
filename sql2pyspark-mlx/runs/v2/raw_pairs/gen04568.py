from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-09-01") & F.hour("paid_at").between(7, 14))
    .select(
        "id",
        "amount",
        "paid_at",
        "order_id",
        F.ceil("amount").alias("amount_whole"),
    )
    .orderBy(F.col("paid_at").desc())
)
