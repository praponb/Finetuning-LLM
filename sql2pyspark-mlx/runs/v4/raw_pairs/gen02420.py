from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/checkout", 1)), F.lit(0)), 2).alias("duration_sec_per_checkout"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
