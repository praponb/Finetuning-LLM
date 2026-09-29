from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "customer_id",
        "plan",
        F.date_format("end_date", "yyyy-MM").alias("end_month"),
    )
    .agg(F.max("end_date").alias("last_end_date"))
)
