from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        F.date_format("start_date", "yyyy-MM").alias("start_date_label"),
    )
)
