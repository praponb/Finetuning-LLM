from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1140, 1190))
    .select(
        "id",
        "status",
        "amount",
        "order_id",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.trim("method").alias("clean_method"),
    )
)
