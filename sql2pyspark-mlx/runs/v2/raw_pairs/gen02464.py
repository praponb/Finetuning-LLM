from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "premium", "standard"]) & (F.col("start_date") >= "2023-10-13") & (F.year("start_date") == 2024))
    .select(
        "id",
        "plan",
        "status",
        F.round("monthly_fee", 1).alias("monthly_fee_rounded"),
    )
)
