from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .withColumn("years_since_opened_date", (F.datediff(F.current_date(), "opened_date") / 365.25))
)

summary = (
    recent_accounts
    .groupBy("status")
    .agg(
        F.countDistinct("years_since_opened_date").alias("unique_statuss"),
        F.avg("years_since_opened_date").alias("avg_years_since_opened_date"),
    )
)

result = (
    summary
    .orderBy("avg_years_since_opened_date", "status")
    .limit(3)
)
