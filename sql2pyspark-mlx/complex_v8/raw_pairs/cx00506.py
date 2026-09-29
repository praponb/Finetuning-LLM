from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_reading_quality = Window.partitionBy("site_zone", "reading_quality")
w_quality_by_reading_kwh = Window.partitionBy("quality").orderBy(F.col("reading_kwh").asc_nulls_last(), "id", "reading_id")
w_quality_by_reading_kwh_ties = Window.partitionBy("quality").orderBy(F.col("reading_kwh").asc_nulls_last())
w_site_zone_by_max_voltage = Window.partitionBy("site_zone").orderBy(F.col("max_voltage").desc(), "id", "reading_id")

filtered_readings = (
    df_meter_readings
    .filter((F.col("voltage") > 223.3) | (F.col("kwh") >= 35.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

site_id_stats = (
    filtered_readings
    .groupBy("site_id")
    .agg(F.max("voltage").alias("max_voltage"))
)

vs_group = (
    filtered_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") == F.col("g.max_voltage"))
    .select(
        "s.*",
        "g.max_voltage",
        F.round(F.col("s.voltage") - F.col("g.max_voltage"), 2).alias("voltage_minus_max"),
    )
)

with_details = (
    vs_group.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") != "Z4")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_clean",
        "s.kwh_filled",
        "s.max_voltage",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

detail_rows = (
    with_details.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_clean",
        "s.max_voltage",
        "s.site_zone",
        "s.site_tariff",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

with_windows = (
    detail_rows
    .select(
        "*",
        F.round(F.col("max_voltage") / F.nullif(F.sum("max_voltage").over(w_site_zone_reading_quality), F.lit(0)), 4).alias("max_voltage_share_of_site_zone_reading_quality"),
        F.lead("reading_read_at", 2).over(w_quality_by_reading_kwh).alias("next2_reading_read_at"),
        F.sum("max_voltage").over(w_site_zone_reading_quality).alias("site_zone_reading_quality_sum_max_voltage"),
        F.rank().over(w_quality_by_reading_kwh_ties).alias("reading_kwh_rank"),
        F.round(F.cume_dist().over(w_site_zone_by_max_voltage), 3).alias("max_voltage_cume_dist"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["reading_kwh_rank", "site_id"], ["reading_kwh_rank"], ["site_id"]],
        "reading_kwh_rank", "site_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("next2_reading_read_at").alias("first_next2_reading_read_at"),
    )
    .orderBy(
        F.col("reading_kwh_rank").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
