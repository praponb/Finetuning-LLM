from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]) | (F.col("status") == "ACTIVE"))
    .groupBy(
        F.quarter("start_date").alias("start_quarter"),
        F.when(F.col("monthly_fee") >= 256, "high").when(F.col("monthly_fee") >= 119, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.max("end_date").alias("last_end_date"))
)
