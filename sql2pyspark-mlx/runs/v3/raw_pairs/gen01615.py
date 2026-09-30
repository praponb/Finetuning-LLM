from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(180, 230) & (F.col("plan").isin(["basic", "enterprise", "standard"]) | F.col("status").isin(["ACTIVE", "EXPIRED"])))
    .select(
        "id",
        "monthly_fee",
        F.date_trunc("YEAR", "end_date").alias("end_date_period"),
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)
