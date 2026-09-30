from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("duration_sec_tier").orderBy(F.col("duration_min").desc())

remaining_keys = (
    df_web_events
    .select("page", "event_type")
    .exceptAll(
        df_web_events
        .filter(F.col("session_id").isin([22, 26]))
        .select("page", "event_type")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("duration_sec") >= 477, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        "session_id",
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .filter(F.col("num_pages") > 1)
    .withColumn("duration_min_percent_rank", F.percent_rank().over(w))
)
