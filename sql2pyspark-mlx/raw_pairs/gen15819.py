from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard"]))
    .groupBy("status")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .orderBy(F.col("monthly_fee_count").desc())
)
