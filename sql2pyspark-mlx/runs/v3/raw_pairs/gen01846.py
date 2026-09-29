from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard"]) & F.month("start_date").isin([2, 4, 7]) & (F.col("status") == "CANCELLED"))
    .select(
        "customer_id",
        "end_date",
        "monthly_fee",
        F.concat("plan", F.lit(" - "), "status").alias("plan_status"),
    )
    .orderBy(F.col("monthly_fee").desc(), F.col("customer_id").desc())
)
