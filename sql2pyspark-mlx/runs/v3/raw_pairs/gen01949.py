from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.year("end_date") == 2024) & F.col("customer_id").isNull()) | F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        F.last_day("end_date").alias("end_date_month_end"),
        F.when(F.col("plan").isin(["standard", "enterprise"]), "Priority").otherwise("Other").alias("plan_group"),
    )
    .orderBy(F.col("plan_group").desc())
)
