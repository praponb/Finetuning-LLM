from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 470)
    .select(
        "id",
        "status",
        "method",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
    .orderBy(F.col("status").desc())
)
