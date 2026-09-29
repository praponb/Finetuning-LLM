from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.month("start_date").isin([5, 8, 9]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

result = (
    valid
    .groupBy("status")
    .pivot("plan", ["enterprise", "standard"])
    .agg(F.max("monthly_fee"))
)
