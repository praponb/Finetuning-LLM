from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "monthly_fee",
        "start_date",
        "plan",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
    .orderBy("plan")
)
