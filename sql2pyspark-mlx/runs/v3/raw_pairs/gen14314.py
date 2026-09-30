from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "status",
        F.date_sub("end_date", 71).alias("end_date_minus_71d"),
        F.length("plan").alias("plan_length"),
    )
    .orderBy("end_date_minus_71d", "id")
    .limit(50)
)
