from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "method",
        "paid_at",
        "order_id",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.substring("status", 1, 2).alias("status_prefix_2"),
    )
)
