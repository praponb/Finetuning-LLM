from pyspark.sql import functions as F

recent_sites = (
    df_sites
    .filter((F.col("installed_kw") < 446.9) & (F.col("commissioned_on").isNotNull()))
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled"),
        F.when(F.col("installed_kw") >= 373.3, "HIGH").when(F.col("installed_kw") >= 96.9, "MEDIUM").otherwise("LOW").alias("installed_kw_tier"),
    )
)

enriched = (
    recent_sites
    .select(
        "*",
        (F.floor(F.col("installed_kw_filled") / 100) * 100).alias("installed_kw_filled_bucket"),
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
    )
)

detail_rows_readings = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.tariff",
        "s.installed_kw",
        "s.commissioned_on",
        "s.installed_kw_filled",
        "s.installed_kw_tier",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

with_details_reading = (
    detail_rows_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .filter(F.col("m.kwh") < 83.8)
    .select(
        "s.id",
        "s.commissioned_on",
        "s.installed_kw_filled",
        "s.installed_kw_tier",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_read_at",
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality_2"),
        F.col("m.site_id").alias("reading_site_id"),
    )
)

result = (
    with_details_reading
    .select(
        "id",
        "reading_id",
        "installed_kw_filled",
        "reading_kwh",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["installed_kw_filled", "reading_kwh"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
