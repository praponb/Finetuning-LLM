from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "end_date",
        F.last_day("start_date").alias("start_date_month_end"),
        F.when(F.col("plan").isin(["premium", "enterprise"]), "Core").otherwise("Other").alias("plan_group"),
    )
)
