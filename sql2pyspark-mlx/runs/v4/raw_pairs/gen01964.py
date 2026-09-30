from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(110, 220) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "customer_id",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
        F.when(F.col("plan").isin(["basic", "standard"]), "Tier 1").otherwise("Other").alias("plan_group"),
    )
    .orderBy("plan_group", "id")
    .limit(10)
)
