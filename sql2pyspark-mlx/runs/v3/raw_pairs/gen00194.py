from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
    .orderBy(F.col("plan_values").desc(), F.col("pct_enterprise").desc())
)
