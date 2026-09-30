from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 28)
    .select(
        "id",
        "amount",
        "status",
        "method",
        F.round("amount", 0).alias("amount_rounded"),
        F.concat("method", F.lit("/"), "status").alias("method_status"),
    )
    .orderBy("method_status")
)
