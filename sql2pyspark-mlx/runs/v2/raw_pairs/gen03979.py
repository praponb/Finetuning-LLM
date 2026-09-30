from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 830) & ((F.col("amount") > 770) | (F.to_date("paid_at") >= "2023-06-23")))
    .select(
        "id",
        "status",
        "order_id",
        F.when(F.col("status") == "FAILED", 1).otherwise(0).alias("is_failed"),
        F.upper("method").alias("method_upper"),
    )
)
