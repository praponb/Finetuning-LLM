from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "PAUSED"]))
    .select(
        "id",
        "plan",
        "end_date",
        "start_date",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)
