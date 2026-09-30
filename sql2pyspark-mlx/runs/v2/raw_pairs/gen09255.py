from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 3, 11]) & ((F.col("end_date") > "2023-08-01") | (F.col("start_date") >= "2025-05-08")))
    .select("id", "start_date", "monthly_fee", "customer_id")
    .orderBy(F.col("id").desc())
)
