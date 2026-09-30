from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") == 2025)
    .groupBy(F.year("end_date").alias("end_year"), "plan")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .filter(F.col("monthly_fee_range") > 44)
    .select("end_year", "plan", "monthly_fee_range")
    .orderBy(F.col("end_year").desc(), "plan")
)
