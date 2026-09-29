from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.count("*").alias("cnt"),
        F.min("start_date").alias("first_start_date"),
    )
    .filter(F.col("cnt") >= 1)
    .orderBy("start_quarter", F.col("first_start_date").desc())
)
