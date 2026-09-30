from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "premium")
    .select(
        "end_date",
        "plan",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
        F.substring("plan", 1, 3).alias("plan_prefix"),
    )
    .orderBy("plan")
)
