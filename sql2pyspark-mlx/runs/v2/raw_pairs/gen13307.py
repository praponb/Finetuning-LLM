from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "CANCELLED") & (F.col("monthly_fee") > 130))
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .orderBy(F.col("monthly_fee_count").desc(), F.col("end_quarter").desc())
)
