from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.year("end_date") >= 2025)
    .select("*", F.dayofmonth("start_date").alias("start_date_day"))
)

result = (
    cleaned
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("plan") == "premium")
        .select("customer_id")
    )
)
