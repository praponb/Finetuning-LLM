from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "standard"]))
    .select(
        "customer_id",
        "status",
        "start_date",
        F.date_format("end_date", "yyyy-MM-dd").alias("end_date_label"),
    )
)
