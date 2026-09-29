from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 39)
    .select(
        "id",
        "end_date",
        "status",
        F.round((F.col("monthly_fee") * 1.07), 2).alias("adjusted_monthly_fee"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)
