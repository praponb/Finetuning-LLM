from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 170)
    .select("end_date", "status")
    .intersect(
        df_subscriptions
        .filter(F.year("start_date") >= 2025)
        .select("end_date", "status")
    )
)
