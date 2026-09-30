from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("customer_id").isin([2, 11, 13, 30]) & F.col("status").isin(["CANCELLED", "ACTIVE", "PAUSED"]))
)

agg = (
    base
    .groupBy(
        F.month("start_date").alias("start_month"),
        F.year("start_date").alias("start_year"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    agg
    .select("start_month", "start_year", "pct_active", "status_values")
    .orderBy(F.col("start_month").desc())
)
