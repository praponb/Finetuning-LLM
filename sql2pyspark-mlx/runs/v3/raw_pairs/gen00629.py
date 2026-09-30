from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id").isin([7, 10, 14, 23]) & (F.year("end_date") >= 2024)) | ~F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]))
    .select(
        "id",
        "plan",
        "end_date",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)
