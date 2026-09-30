from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") != "paypal") & (F.col("paid_at") >= "2023-05-01"))
    .select(
        "order_id",
        F.when(F.col("amount") >= 1390, "High").when(F.col("amount") >= 470, "Medium").otherwise("Low").alias("amount_tier"),
    )
)
