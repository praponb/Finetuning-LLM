from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "status",
        "amount",
        "order_id",
        F.concat("method", F.lit(" - "), "status").alias("method_status"),
    )
)
