from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("event_time") >= "2024-06-01") & (F.col("duration_sec") > 233))
)

event_month_summary = (
    filtered_web_events
    .groupBy(
        F.date_trunc("MONTH", "event_time").alias("event_month"),
        F.when(F.col("duration_sec") >= 402, "high").when(F.col("duration_sec") >= 225, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = (
    event_month_summary
    .orderBy(F.col("pct_add_to_cart").desc(), "duration_sec_bucket", "event_month")
    .limit(20)
)
