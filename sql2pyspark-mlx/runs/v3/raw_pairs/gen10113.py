from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 4, 5]) & ((F.col("plan") != "basic") | (F.col("start_date") < "2024-05-28")))
    .groupBy("status")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
    .orderBy(F.col("plan_values").desc())
)
