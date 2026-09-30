from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 375, "high").when(F.col("duration_sec") >= 172, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
    .filter(F.col("unique_event_types") >= 9)
    .select("duration_sec_bucket", "event_type_values", "unique_event_types")
)
