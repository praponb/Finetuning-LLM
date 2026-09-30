from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(170, 180))
    .select(
        "id",
        "status",
        "customer_id",
        "monthly_fee",
        F.date_format("end_date", "yyyy-MM").alias("end_date_label"),
    )
)
