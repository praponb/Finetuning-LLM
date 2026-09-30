from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "plan",
        "customer_id",
        F.round((F.col("monthly_fee") * 1.15), 2).alias("monthly_fee_scaled"),
    )
    .orderBy("monthly_fee_scaled")
)
