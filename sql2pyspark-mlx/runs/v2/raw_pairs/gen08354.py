from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
    .filter(F.col("basic_count") >= 7)
    .select("plan", "monthly_fee_range", "basic_count")
)
