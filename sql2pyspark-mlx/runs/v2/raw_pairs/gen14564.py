from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(170, 190))
    .select("end_date", "customer_id")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") > "2025-09-01")
        .select("end_date", "customer_id")
    )
)
