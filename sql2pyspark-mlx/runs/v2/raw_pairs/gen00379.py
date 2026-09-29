from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2024-02-01")
    .select(
        "id",
        "end_date",
        "monthly_fee",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
    .orderBy("end_date", F.col("monthly_fee").desc())
)
