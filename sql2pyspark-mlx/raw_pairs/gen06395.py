from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "id",
        "method",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.substring("status", 1, 4).alias("status_prefix"),
    )
    .orderBy(F.col("days_since_paid_at").desc())
)
