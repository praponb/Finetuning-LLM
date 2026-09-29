from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") >= "2024-08-01") & ~F.col("status").isin(["EXPIRED", "CANCELLED"]) & (F.col("end_date") <= "2025-05-01"))
    .groupBy(
        "status",
        F.when(F.col("monthly_fee") >= 244, "high").when(F.col("monthly_fee") >= 114, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("start_date").alias("min_start_date"),
    )
    .filter(F.col("enterprise_count") > 33)
    .orderBy(F.col("status").desc(), "min_start_date")
)
