from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("event_type")
    .exceptAll(
        df_web_events
        .filter(F.col("page").isin(["/search", "/product", "/checkout"]))
        .select("event_type")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("duration_sec") >= 377, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.avg(F.when(F.col("event_type") == "page_view", F.col("duration_sec"))).alias("page_view_avg_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("event_time").alias("ended_at"),
    )
    .orderBy("page_view_avg_duration_sec")
)
