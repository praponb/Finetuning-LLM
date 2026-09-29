from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([7, 13, 14, 21]) | F.col("amount").between(620, 1000))
    .select(
        "id",
        "amount",
        "order_id",
        "status",
        F.floor("amount").alias("amount_whole"),
    )
)
