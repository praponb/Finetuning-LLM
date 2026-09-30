from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        F.concat("method", F.lit(" - "), "status").alias("method_status"),
    )
)
