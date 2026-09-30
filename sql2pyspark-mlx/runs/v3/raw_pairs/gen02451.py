from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-01-01")
    .select(
        "id",
        "order_id",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.concat("status", F.lit(" "), "method").alias("status_method"),
    )
)
