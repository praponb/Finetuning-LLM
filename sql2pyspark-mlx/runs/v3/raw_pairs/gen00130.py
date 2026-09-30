from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.year("end_date") >= 2025) & (F.col("start_date") < "2023-07-01"))
    .select(
        "status",
        "customer_id",
        F.round((F.col("monthly_fee") * 0.9), 2).alias("monthly_fee_scaled"),
    )
)
