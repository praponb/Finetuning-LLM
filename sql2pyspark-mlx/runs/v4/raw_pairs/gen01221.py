from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 100)
    .groupBy("plan")
    .agg(
        F.min("end_date").alias("first_end_date"),
        F.min("start_date").alias("first_start_date"),
    )
    .orderBy("first_end_date")
)
