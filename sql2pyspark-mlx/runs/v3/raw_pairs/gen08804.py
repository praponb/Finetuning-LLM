from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() | ~F.col("plan").isin(["standard", "premium", "basic"]))
    .select(
        "id",
        "end_date",
        "customer_id",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
    .orderBy(F.col("end_date").desc())
)
