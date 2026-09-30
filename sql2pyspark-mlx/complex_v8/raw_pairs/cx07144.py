from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_read_at_full = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality = Window.partitionBy("quality")

base_readings = (
    df_meter_readings
    .filter((F.col("quality").isin("EST", "OK")) & (F.col("kwh") <= 96.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

with_windows = (
    base_readings
    .select(
        "*",
        F.nth_value("voltage", 2).over(w_quality_by_read_at_full).alias("second_voltage"),
        F.ntile(5).over(w_quality_by_read_at).alias("read_at_quintile"),
        F.round(F.col("kwh_bucket") - F.avg("kwh_bucket").over(w_quality), 2).alias("kwh_bucket_vs_quality_avg"),
        F.sum("kwh_bucket").over(w_quality).alias("quality_sum_kwh_bucket"),
    )
)

combined_details_site = (
    with_windows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") <= 386.6)
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

site_id_stats = (
    combined_details_site
    .groupBy("site_id")
    .agg(F.avg("read_at_quintile").alias("avg_read_at_quintile"))
)

outliers = (
    combined_details_site.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.read_at_quintile") > F.col("g.avg_read_at_quintile"))
    .select(
        "s.*",
        "g.avg_read_at_quintile",
        F.round(F.col("s.read_at_quintile") - F.col("g.avg_read_at_quintile"), 2).alias("read_at_quintile_minus_avg"),
    )
)

with_children = (
    outliers.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.days_since_read_at",
        "s.kwh_bucket",
        "s.quality_sum_kwh_bucket",
        "s.site_name",
        "s.read_at_quintile_minus_avg",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    with_children
    .groupBy("site_id", "reading_read_at")
    .pivot("quality", ["OK", "EST"])
    .agg(F.min("reading_voltage").alias("min_v"), F.sum("reading_voltage").alias("sum_v"))
)
