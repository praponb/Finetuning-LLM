from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2024-10-30")
    .select(
        "id",
        "start_date",
        "plan",
        "end_date",
        F.length("plan").alias("plan_length"),
    )
    .orderBy("plan", "id")
)
