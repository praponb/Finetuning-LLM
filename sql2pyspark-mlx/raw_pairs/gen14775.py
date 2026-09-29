from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([2, 6, 9]) & (F.col("customer_id").isNotNull() | (F.col("start_date") < "2024-04-01")))
    .select(
        "plan",
        "start_date",
        "status",
        F.date_add("end_date", 3).alias("end_date_plus_3d"),
    )
)
