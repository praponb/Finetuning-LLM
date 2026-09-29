from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "standard") & (F.col("status").isin(["EXPIRED", "ACTIVE"]) | (F.col("monthly_fee") <= 170)))
    .select(
        "id",
        "monthly_fee",
        "customer_id",
        F.concat("plan", F.lit(", "), "status").alias("plan_status"),
    )
)
