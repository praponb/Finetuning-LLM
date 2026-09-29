from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter((F.col("plan") != "enterprise") & F.col("customer_id").isNotNull())
    .withColumn("status_plan_key", F.concat_ws("/", "status", "plan"))
)

result = (
    cleaned
    .select("id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") < "2024-03-01")
        .select("id")
    )
)
