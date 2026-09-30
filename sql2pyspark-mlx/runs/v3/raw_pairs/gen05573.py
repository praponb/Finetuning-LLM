from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .groupBy(
        F.when(F.col("duration_sec") >= 391, "high").when(F.col("duration_sec") >= 102, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.date_trunc("MONTH", "event_time").alias("event_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
    .filter(F.col("pct_search") >= 40)
)
