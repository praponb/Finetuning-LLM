from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(~F.col("plan").isin(["basic", "premium"]) | (F.col("monthly_fee") <= 80))
)

agg = (
    base
    .groupBy("status")
    .agg(F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"))
)

result = (
    agg
    .orderBy("standard_count", "status")
    .limit(20)
)
