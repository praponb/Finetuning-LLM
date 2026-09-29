from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 170)
    .select(
        "id",
        "start_date",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)
