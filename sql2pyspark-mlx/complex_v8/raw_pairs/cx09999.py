from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_last_reading_read_at = Window.partitionBy("quality", F.month("last_reading_read_at")).orderBy("read_at", "id")

base_readings = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(9, 11))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("reading_quality_count"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_counts = (
    base_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.last_reading_read_at",
    )
)

metrics = (
    with_counts
    .select(
        "*",
        F.round(F.col("reading_quality_count") / F.nullif(F.sum("reading_quality_count").over(w_site_id), F.lit(0)), 4).alias("reading_quality_count_share_of_site_id"),
        F.round((F.unix_timestamp("last_reading_read_at") - F.unix_timestamp(F.lag("last_reading_read_at").over(w_quality_last_reading_read_at))) / 60, 1).alias("minutes_since_prev_last_reading_read_at"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
    )
)

site_id_stats = (
    metrics
    .groupBy("site_id")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

vs_group = (
    metrics.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["reading_quality_count", "quality", "site_id"], ["reading_quality_count"]],
        "reading_quality_count", "quality", "site_id",
    )
    .agg(
        F.sum(F.when(~F.col("quality").isin("OK", "EST"), F.col("minutes_since_prev_last_reading_read_at")).otherwise(0)).alias("minutes_since_prev_last_reading_read_at_quality_"),
        F.round(F.sum("avg_kwh") / F.nullif(F.sum("reading_quality_count"), F.lit(0)), 3).alias("avg_kwh_to_reading_quality_count_ratio"),
    )
    .orderBy(
        F.col("reading_quality_count").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
