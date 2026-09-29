from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-07-19").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        F.date_add("start_date", 58).alias("start_date_plus_58d"),
    )
)

result = (
    cleaned
    .select("status", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee").between(80, 180))
        .select("status", "id")
    )
)
