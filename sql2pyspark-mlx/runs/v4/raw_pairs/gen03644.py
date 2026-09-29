from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-03-02", "2025-10-21"))
    .select(
        "*",
        F.length("plan").alias("plan_length"),
        F.trim("plan").alias("clean_plan"),
    )
)

result = (
    base
    .groupBy("customer_id", "status")
    .pivot("plan", ["premium", "enterprise"])
    .agg(F.avg("plan_length").alias("amount"), F.count(F.lit(1)).alias("n"))
)
