from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .select(
        "id",
        "customer_id",
        "plan",
        F.date_trunc("week", "end_date").alias("end_date_period"),
    )
)
