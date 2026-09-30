from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "method",
        "order_id",
        "status",
        F.concat_ws("/", "method", "status").alias("method_status_key"),
    )
    .orderBy("status")
)
