from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-07-13") & (F.col("order_id") == 29))
    .select(
        "id",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.quarter("paid_at").alias("paid_at_quarter"),
    )
)
