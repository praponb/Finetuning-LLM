from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("country", "device")
    .intersect(
        df_sessions
        .filter(F.col("country") == "TH")
        .select("country", "device")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"), "browser")
    .agg(
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.max("started_at").alias("ended_at"),
    )
    .orderBy("activity_month")
)
