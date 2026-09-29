from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "order_id",
        F.concat("status", F.lit(" "), "method").alias("status_method"),
    )
)
