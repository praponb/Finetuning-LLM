from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([5, 7, 12]))
    .select(
        "id",
        "end_date",
        "start_date",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)
