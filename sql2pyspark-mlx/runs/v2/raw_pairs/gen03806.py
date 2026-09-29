from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("plan") != "enterprise") & (F.year("start_date") >= 2024))
)

agg = (
    base
    .groupBy(F.year("end_date").alias("end_year"))
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
)

result = (
    agg
    .select("end_year", "monthly_fee_range")
    .orderBy(F.col("monthly_fee_range").desc(), F.col("end_year").desc())
)
