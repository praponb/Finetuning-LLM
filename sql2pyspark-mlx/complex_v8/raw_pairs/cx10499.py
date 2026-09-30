from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_quality_running))

quality_stats = (
    sessionized
    .groupBy("quality")
    .agg(F.avg("new_session_flag").alias("avg_new_session_flag"))
)

above_average = (
    sessionized.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.new_session_flag") > F.col("g.avg_new_session_flag"))
    .select(
        "s.*",
        "g.avg_new_session_flag",
        F.round(F.col("s.new_session_flag") - F.col("g.avg_new_session_flag"), 2).alias("new_session_flag_minus_avg"),
    )
)

with_details = (
    above_average.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality_group",
        "s.prev_read_at",
        "s.session_id",
        "s.new_session_flag_minus_avg",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_details
    .groupBy("session_id")
    .pivot("quality_group", ["group_a", "group_b"])
    .agg(F.sum("new_session_flag_minus_avg"))
    .orderBy("session_id")
)
