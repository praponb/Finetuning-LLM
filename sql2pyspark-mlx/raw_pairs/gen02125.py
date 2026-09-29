from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([5, 9, 11]))
    .select(
        "start_date",
        "plan",
        "customer_id",
        F.when(F.col("status") == "CANCELLED", 1).otherwise(0).alias("is_cancelled"),
    )
)
