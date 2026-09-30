from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "plan",
        "end_date",
        F.concat("plan", F.lit(", "), "status").alias("plan_status"),
        F.add_months("start_date", 3).alias("start_date_plus_months"),
    )
)
