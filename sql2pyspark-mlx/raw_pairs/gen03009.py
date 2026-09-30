from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-07-19")))
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
)

result = (
    cleaned
    .groupBy("status")
    .pivot("plan", ["premium", "standard"])
    .agg(F.max("monthly_fee_int").alias("amount"))
)
