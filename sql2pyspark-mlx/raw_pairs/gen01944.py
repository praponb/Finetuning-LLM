from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser")

base = (
    df_sessions
    .filter(F.col("started_at") > F.to_timestamp(F.lit("2023-09-20 05:00:00")))
    .withColumn("browser_device_key", F.concat_ws("-", "browser", "device"))
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "browser",
        "started_at",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

not_flagged = (
    islands
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page").isNotNull()), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "id",
        "browser",
        F.stddev_samp("session_id").over(w3).alias("browser_stddev_samp_session_id"),
        F.min("session_id").over(w3).alias("browser_min_session_id"),
    )
)

result = metrics.select("id", "browser")
