from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "status",
        "method",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.date_trunc("WEEK", "paid_at").alias("paid_at_period"),
    )
)
