from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("kwh") >= 101.2, "large").when(F.col("kwh") >= 35.9, "medium").otherwise("small").alias("kwh_tier"),
    )
)

with_prev = filtered_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_quality_running))

quality_stats = (
    sessionized
    .groupBy("quality")
    .agg(F.max("kwh").alias("max_kwh"))
)

outliers = (
    sessionized.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

tagged = (
    outliers
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "kwh_tier",
        "prev_read_at",
        "session_id",
        "max_kwh",
        "kwh_minus_max",
        F.length(F.trim(F.col("kwh_tier"))).alias("kwh_tier_len"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.concat_ws("-", F.col("kwh_tier"), F.col("quality")).alias("kwh_tier_quality_label"),
    )
)

result = (
    tagged
    .select("id", "kwh_tier_len", "session_id")
    .unpivot(
        ids=["id"],
        values=["kwh_tier_len", "session_id"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
