from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("amount") > 1440) & F.hour("paid_at").between(12, 15))
    .select(
        "id",
        "status",
        "order_id",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.ceil("amount").alias("amount_whole"),
    )
)
