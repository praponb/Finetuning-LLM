from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2024-03-25").cast("date")) & F.col("customer_id").isNotNull() & F.col("end_date").between("2024-06-01", "2024-09-01"))
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.dayofweek("end_date").alias("end_date_dayofweek"),
    )
)

result = (
    base
    .cube("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("end_date_dayofweek")).otherwise(0)).alias("expired_end_date_dayofweek"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.round(F.sum("end_date_dayofweek") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("end_date_dayofweek_per_premium"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "ACTIVE", 1)), F.lit(0)), 2).alias("monthly_fee_per_active"),
        F.count_if(F.col("plan") == "premium").alias("premium_events"),
    )
    .orderBy(F.col("expired_end_date_dayofweek").desc())
)
