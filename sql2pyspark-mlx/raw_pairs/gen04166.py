from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("page", "country")
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())
w3 = Window.partitionBy("session_id", "event_type").orderBy("duration_sec_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type", "session_id")

valid = (
    df_web_events
    .filter(F.col("event_time") >= "2023-08-01")
    .withColumn("event_time_date", F.to_date("event_time"))
)

enriched = (
    valid
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.country"))
)

metrics = (
    enriched
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.stddev_samp("duration_sec").over(w_ordered).alias("page_stddev_samp_duration_sec"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
    )
)

metrics_web_events = (
    metrics
    .filter(F.col("duration_sec_cume_dist") >= 0.73)
    .select(
        "*",
        F.sum("duration_sec_cume_dist").over(w3).alias("running_duration_sec_cume_dist"),
        F.min("page_stddev_samp_duration_sec").over(w4).alias("event_type_min_page_stddev_samp_duration_sec"),
    )
)

result = (
    metrics_web_events
    .groupBy(
        F.when(F.col("duration_sec_cume_dist") >= 0.77, "HIGH").otherwise("LOW").alias("duration_sec_cume_dist_tier"),
        F.when(F.col("page_stddev_samp_duration_sec") >= 470, "PREMIUM").otherwise("STANDARD").alias("page_stddev_samp_duration_sec_tier"),
    )
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.percentile_approx("event_type_min_page_stddev_samp_duration_sec", [0.25, 0.5, 0.75]).alias("event_type_min_page_stddev_samp_duration_sec_quartiles"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
    .filter(F.col("purchase_count") > 1)
    .orderBy(F.col("purchase_count").asc_nulls_first())
)
