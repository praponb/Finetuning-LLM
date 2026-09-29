from pyspark.sql import functions as F

src = df_subscriptions.filter(F.year("start_date") == 2025)

agg = (
    src
    .groupBy("status")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.min("start_date").alias("min_start_date"),
    )
)

result = (
    agg
    .filter(F.col("min_monthly_fee") > 200)
    .select("status", "min_monthly_fee", "min_start_date")
    .orderBy(F.col("min_monthly_fee").desc(), "min_start_date")
)
