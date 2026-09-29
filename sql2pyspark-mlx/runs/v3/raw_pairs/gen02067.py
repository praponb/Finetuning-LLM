from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.col("status") != "EXPIRED") & (F.col("status") == "ACTIVE")) | (F.col("plan") == "enterprise"))
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .filter(F.col("total_monthly_fee") > 1060)
    .orderBy(F.col("total_monthly_fee").desc())
)
