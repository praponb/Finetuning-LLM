from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 33)
    .select(
        "id",
        "amount",
        F.concat("method", F.lit("/"), "status").alias("method_status"),
        F.upper("status").alias("status_upper"),
    )
    .orderBy("amount")
)
