from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 220)
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
    .orderBy(F.col("enterprise_monthly_fee").desc())
)
