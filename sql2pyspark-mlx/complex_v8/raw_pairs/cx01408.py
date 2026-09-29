from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality = Window.partitionBy("reading_quality")
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_meter_readings
    .filter((F.col("kwh") > 30.2) & (F.col("quality").isin("OK", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.abs(F.col("kwh") - 63.4).alias("kwh_distance"),
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_details_site = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.read_at_date",
        "s.quality_group",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

detail_rows_readings = (
    with_details_site.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.site_zone",
        "s.site_tariff",
        "s.site_installed_kw",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

combined_details_reading = (
    detail_rows_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("m.site_id"), "left")
    .filter(F.col("m.quality").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.site_zone",
        "s.site_installed_kw",
        "s.reading_id",
        "s.reading_kwh",
        "s.reading_quality",
        F.col("m.quality").alias("reading_quality_2"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

sequenced = (
    combined_details_reading
    .select(
        "*",
        F.max("site_installed_kw").over(w_reading_quality).alias("reading_quality_max_site_installed_kw"),
        F.count(F.lit(1)).over(w_reading_quality).alias("rows_in_reading_quality"),
        F.min("reading_kwh").over(w_all_by_site_commissioned_on).alias("min_reading_kwh_so_far"),
        F.size(F.collect_set("reading_quality_2").over(w_reading_quality)).alias("distinct_reading_quality_2_in_reading_quality"),
    )
)

result = (
    sequenced
    .filter(F.col("site_zone") != "Z4")
    .select(
        "id",
        "site_id",
        "site_zone",
        "site_installed_kw",
        "reading_id",
        "reading_kwh",
        "reading_quality",
        "reading_quality_2",
        "site_commissioned_on",
        "reading_quality_max_site_installed_kw",
        "rows_in_reading_quality",
        "min_reading_kwh_so_far",
        "distinct_reading_quality_2_in_reading_quality",
        F.lower(F.trim(F.col("reading_quality"))).alias("reading_quality_clean"),
        F.abs(F.col("site_installed_kw") - 141.6).alias("site_installed_kw_distance"),
    )
)
