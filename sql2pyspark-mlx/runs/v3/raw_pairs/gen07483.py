from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(80, 100))
    .groupBy("plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
    .filter(F.col("pct_premium") < 37)
    .select("plan", "pct_premium")
    .orderBy(F.col("plan").desc(), F.col("pct_premium").desc())
)
