from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("pct_cancelled") > 48)
    .orderBy(F.col("pct_cancelled").desc(), "plan")
    .limit(5)
)
