from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-04-15").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("plan_title", F.initcap(F.trim("plan")))
    .withColumn("plan_status_key", F.concat_ws("-", "plan", "status"))
)

result = (
    filtered
    .select("plan")
    .subtract(
        df_subscriptions
        .filter(F.col("status") != "EXPIRED")
        .select("plan")
    )
)
