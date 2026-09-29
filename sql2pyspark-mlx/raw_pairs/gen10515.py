from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((~F.col("browser").eqNullSafe(F.lit("Firefox"))) & (F.col("browser") == "Edge"))
    .withColumn("browser_device_key", F.concat_ws("-", "browser", "device"))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("OTHER")))
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    gaps
    .select(
        "id",
        "started_at",
        "country",
        "browser_device_key",
        "resolved_browser",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    sessions_sessions
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"), "session_id")
    .agg(
        F.avg(F.when(F.col("country") == "SG", F.col("session_id"))).alias("sg_avg_session_id"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows_2"),
        F.sum(F.when(F.col("country") == "SG", F.col("session_id"))).alias("sg_total_session_id"),
    )
    .orderBy("week_start")
)
