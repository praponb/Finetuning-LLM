from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("monthly_fee") < 150))
    .select(
        "id",
        "start_date",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
    .orderBy("start_date")
)
