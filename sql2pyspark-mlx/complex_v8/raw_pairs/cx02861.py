from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_quality = Window.partitionBy("read_at_year", "quality")
w_read_at_year_by_median_kwh_f = Window.partitionBy("read_at_year").orderBy(F.col("median_kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year_by_median_kwh = Window.partitionBy("read_at_year").orderBy(F.col("median_kwh").desc(), "id")

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label_2"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

quality_group_stats = (
    cleaned
    .groupBy("quality_group")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

vs_group = (
    cleaned.alias("s")
    .join(quality_group_stats.alias("g"), F.col("g.quality_group") == F.col("s.quality_group"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

filtered = (
    vs_group.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 231.6), "left_semi")
)

enriched_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") < F.lit("2025-05-10").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_label",
        "s.read_at_label_2",
        "s.quality_group",
        "s.median_kwh",
        "s.kwh_minus_median",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

enriched_site_2 = (
    enriched_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_label",
        "s.read_at_label_2",
        "s.quality_group",
        "s.median_kwh",
        "s.kwh_minus_median",
        "s.site_zone",
        "s.site_tariff",
        "s.site_commissioned_on",
        "si.site_name",
        F.col("si.zone").alias("site_zone_2"),
    )
)

enriched = (
    enriched_site_2
    .filter(F.col("site_tariff").isin("TOU", "EV"))
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "read_at_label_2",
        "quality_group",
        "median_kwh",
        "kwh_minus_median",
        "site_commissioned_on",
        "site_name",
        F.year(F.col("read_at")).alias("read_at_year"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "quality_group",
        "median_kwh",
        "kwh_minus_median",
        "site_commissioned_on",
        "site_name",
        F.min("read_at_year").over(w_read_at_year_quality).alias("read_at_year_quality_min_read_at_year"),
        F.nth_value("read_at", 2).over(w_read_at_year_by_median_kwh_f).alias("second_read_at"),
        F.row_number().over(w_read_at_year_by_median_kwh).alias("median_kwh_seq"),
    )
)

result = (
    with_windows
    .select("quality")
    .distinct()
    .orderBy(F.col("quality").desc_nulls_first())
)
