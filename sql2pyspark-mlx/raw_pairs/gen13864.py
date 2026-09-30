from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-12").cast("date")) & (F.col("country") == "DE"))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.concat_ws("-", "country", "device").alias("country_device_key"),
    )
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    flagged
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_sessions
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 8, "LARGE").otherwise("SMALL"))
    .rollup("session_no_tier", "device")
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("session_no_per_desktop"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("device") == "tablet", 1)), F.lit(0)), 2).alias("session_no_per_tablet"),
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.count("*").alias("row_count"),
        F.countDistinct("country").alias("distinct_countries"),
        F.grouping("session_no_tier").alias("gid"),
    )
    .orderBy(F.col("row_count").desc())
)
