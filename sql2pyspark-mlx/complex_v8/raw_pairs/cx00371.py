from pyspark.sql import functions as F

prepared = (
    df_sites
    .filter(F.col("tariff") == "EV")
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.date_format(F.col("commissioned_on"), "yyyy-MM").alias("commissioned_on_label"),
        F.abs(F.col("installed_kw") - 141.6).alias("installed_kw_distance"),
    )
)

with_children_readings = (
    prepared.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.site_name",
        "s.commissioned_on",
        "s.commissioned_on_label",
        "s.installed_kw_distance",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

qualified = (
    with_children_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.month(F.col("m.read_at")).isin(2, 3)) & (F.col("m.quality") == "EST"), "left_semi")
)

enriched = (
    qualified
    .select(
        "id",
        "site_name",
        "commissioned_on",
        "reading_id",
        "reading_quality",
        "reading_read_at",
        F.concat(F.col("reading_quality"), F.lit("#"), F.col("id").cast("string")).alias("reading_quality_ref"),
        F.date_format(F.col("reading_read_at"), "yyyy-MM-dd HH:00").alias("reading_read_at_label"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "site_name",
        "commissioned_on",
        "reading_id",
        "reading_quality",
        "reading_read_at",
        "reading_quality_ref",
        "reading_read_at_label",
        "max_reading_voltage",
        "reading_count",
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
        F.concat_ws(": ", F.col("reading_quality_ref"), F.col("reading_read_at_label")).alias("reading_quality_ref_reading_read_at_label_label"),
    )
    .orderBy(F.col("reading_id").desc())
)
