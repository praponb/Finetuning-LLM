from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-05-28").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.concat("plan", F.lit(" "), "status").alias("plan_status"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

result = (
    filtered
    .select("plan")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date") > "2025-03-01")
        .select("plan")
    )
)
