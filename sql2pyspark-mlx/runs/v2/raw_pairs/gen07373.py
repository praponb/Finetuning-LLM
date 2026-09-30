from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([12, 18, 27, 37]))
    .select(
        "id",
        "status",
        "end_date",
        "customer_id",
        F.date_format("end_date", "MMM yyyy").alias("end_date_label"),
    )
)
