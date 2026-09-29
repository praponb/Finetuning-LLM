from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        "end_date",
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
    .orderBy("end_date", "id")
    .limit(3)
)
