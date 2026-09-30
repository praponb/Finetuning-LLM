from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "ACTIVE"]) & ~F.col("status").isin(["PAUSED", "ACTIVE", "EXPIRED"]) & (F.col("end_date") < "2025-04-01"))
    .select(
        "id",
        "start_date",
        "customer_id",
        "status",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
    .orderBy(F.col("customer_id").desc())
)
