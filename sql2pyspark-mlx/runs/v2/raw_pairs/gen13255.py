from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & F.col("status").isin(["SUCCESS", "FAILED"]))
    .select(
        "id",
        "order_id",
        "status",
        "method",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)
