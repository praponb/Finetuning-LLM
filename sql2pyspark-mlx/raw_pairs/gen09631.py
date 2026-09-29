from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2025-03-08", "2025-07-01") & (F.col("start_date") <= "2025-08-06") & (F.col("plan") == "premium"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.count("*").alias("total_count"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)
