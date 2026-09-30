from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select(
        "method",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.date_trunc("week", "paid_at").alias("paid_at_period"),
    )
    .orderBy("method")
)
