from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "enterprise"]))
    .select(
        "id",
        "customer_id",
        "end_date",
        "start_date",
        F.date_format("start_date", "yyyy-MM").alias("start_date_label"),
        F.month("end_date").alias("end_date_month"),
    )
    .orderBy(F.col("start_date").desc())
)
