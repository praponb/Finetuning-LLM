from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "PAUSED", "ACTIVE"]))
    .groupBy("plan")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.min("end_date").alias("first_end_date"),
    )
    .filter(F.col("unique_plans") > 6)
    .select("plan", "unique_plans", "first_end_date")
    .orderBy("first_end_date")
)
