from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "EXPIRED", "ACTIVE"]) & F.col("plan").isin(["premium", "basic", "standard"]))
    .groupBy(
        F.date_format("end_date", "yyyy-MM").alias("end_month"),
        F.year("end_date").alias("end_year"),
        "plan",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.max("end_date").alias("latest_end_date"),
    )
)
