from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE", "CANCELLED"]) & (F.col("status") == "EXPIRED"))
    .select(
        "monthly_fee",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.upper("plan").alias("plan_upper"),
    )
)
