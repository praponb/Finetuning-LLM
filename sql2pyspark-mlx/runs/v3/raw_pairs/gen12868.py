from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 200) | F.col("status").isin(["EXPIRED", "CANCELLED"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        F.date_sub("start_date", 65).alias("start_date_minus_65d"),
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)
