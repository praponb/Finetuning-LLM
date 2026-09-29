from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE"]) & F.col("plan").isin(["premium", "enterprise", "basic"]))
    .select(
        "id",
        "start_date",
        "status",
        "monthly_fee",
        F.round((F.col("monthly_fee") * 1.07), 2).alias("monthly_fee_scaled"),
    )
)
