from pyspark.sql import functions as F

src = df_subscriptions.filter(F.col("end_date") <= "2024-10-07")

agg = (
    src
    .groupBy("plan")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    agg
    .filter(F.col("min_monthly_fee") >= 130)
    .select("plan", "min_monthly_fee")
    .orderBy("plan", F.col("min_monthly_fee").desc())
)
