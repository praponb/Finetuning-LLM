from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 110)
    .select("plan", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") > "2024-05-01")
        .select("plan", "end_date")
    )
)
