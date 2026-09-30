from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("start_date") > F.to_date(F.lit("2023-10-10")))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.substring("status", 1, 4).alias("status_prefix"),
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

result = (
    base
    .groupBy("status")
    .pivot("plan", ["enterprise", "standard"])
    .agg(F.sum("monthly_fee").alias("total"))
)
