from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "PAUSED"]) & (F.col("status") == "CANCELLED"))
    .groupBy("plan")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.min("end_date").alias("first_end_date"),
    )
    .filter(F.col("unique_plans") > 2)
    .orderBy(F.col("plan").desc())
    .limit(5)
)
