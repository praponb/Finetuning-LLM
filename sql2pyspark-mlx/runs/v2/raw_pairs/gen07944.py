from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED"]) & (F.col("end_date") >= "2023-09-09") & F.col("status").isin(["PAUSED", "EXPIRED", "ACTIVE"]))
    .select(
        "plan",
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
        F.round("monthly_fee", 2).alias("monthly_fee_rounded"),
    )
)
