from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2025)
    .select("end_date", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") >= "2023-08-01")
        .select("end_date", "status")
    )
)
