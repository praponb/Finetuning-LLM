from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "enterprise"]) & (F.col("customer_id") == 2) & (F.col("start_date") < "2024-08-01"))
    .select(
        "id",
        "monthly_fee",
        "end_date",
        F.round((F.datediff("start_date", "end_date") / 30), 1).alias("months_end_date_to_start_date"),
    )
)
