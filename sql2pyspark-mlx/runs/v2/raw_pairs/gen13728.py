from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 90) | (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "end_date",
        "start_date",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.upper("plan").alias("plan_upper"),
    )
)
