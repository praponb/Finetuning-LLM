from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 170) & ((F.col("start_date") > "2024-08-07") | (F.col("end_date") <= "2024-03-01")))
    .select(
        "status",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date_2"),
    )
    .orderBy("status")
)
