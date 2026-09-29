from pyspark.sql import functions as F

base = df_web_events.filter(F.col("event_type") != "add_to_cart")

summary = (
    base
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
)

result = (
    summary
    .filter(F.col("unique_sessions") > 9)
    .orderBy("event_type_values")
)
