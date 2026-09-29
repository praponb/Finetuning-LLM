from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([3, 5, 7]) & (F.col("end_date") > "2024-09-01"))
    .select("status", "end_date")
    .orderBy(F.col("end_date").desc())
)
