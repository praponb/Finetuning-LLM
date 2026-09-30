from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED"]) | F.col("order_id").isin([26, 30]))
    .select(
        "id",
        "method",
        F.concat_ws(",", "status", "method").alias("status_method_key"),
        F.upper("status").alias("status_upper"),
    )
)
