from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2024-02-04").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

result = (
    cleaned
    .select("customer_id", "id")
    .intersect(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]))
        .select("customer_id", "id")
    )
)
