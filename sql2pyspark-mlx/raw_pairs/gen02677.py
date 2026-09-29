from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "end_date",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.trim("plan").alias("clean_plan"),
    )
)
