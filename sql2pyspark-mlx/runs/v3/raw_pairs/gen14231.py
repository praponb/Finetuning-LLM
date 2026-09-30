from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "standard", "enterprise"]) & F.col("end_date").between("2023-08-01", "2025-05-01"))
    .select(
        "id",
        "monthly_fee",
        F.date_sub("start_date", 31).alias("start_date_minus_31d"),
        F.concat_ws("/", "plan", "status").alias("plan_status_key"),
    )
)
