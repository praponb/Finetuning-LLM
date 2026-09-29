from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        F.upper("status").alias("status_upper"),
        F.concat_ws("-", "status", "method").alias("status_method_key"),
    )
)
