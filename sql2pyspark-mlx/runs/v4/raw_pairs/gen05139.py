from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2024-01-12").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("plan_lower", F.lower("plan"))
)

result = (
    cleaned
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") > "2025-10-01")
        .select("customer_id")
    )
)
