from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_sessions
    .rollup(
        F.when(F.col("island_id") >= 7, "LARGE").otherwise("SMALL").alias("island_id_tier"),
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.avg("island_id").alias("avg_island_id"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
)
