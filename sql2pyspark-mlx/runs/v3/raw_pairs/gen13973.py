from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "CANCELLED"]))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "plan",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)
