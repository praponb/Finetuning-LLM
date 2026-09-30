from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "status",
        "paid_at",
        "order_id",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
    .orderBy(F.col("status").desc())
)
