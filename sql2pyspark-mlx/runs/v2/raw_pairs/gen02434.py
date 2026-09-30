from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 200)
    .select("end_date", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date").between("2024-04-01", "2025-08-28"))
        .select("end_date", "status")
    )
    .orderBy("end_date")
)
