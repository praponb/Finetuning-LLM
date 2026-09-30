from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "enterprise")
    .select(
        "status",
        "start_date",
        "plan",
        (F.datediff(F.current_date(), "end_date") / 365.25).alias("years_since_end_date"),
    )
)
