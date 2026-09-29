from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNull())
    .select(
        "id",
        "status",
        F.date_format("end_date", "yyyy-MM-dd").alias("end_date_label"),
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)
