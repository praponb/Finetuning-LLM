from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_tariff_by_installed_kw = Window.partitionBy("zone", "tariff").orderBy("installed_kw")
w_zone_tariff_by_installed_kw_2 = Window.partitionBy("zone", "tariff").orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_zone_tariff = Window.partitionBy("zone", "tariff")
w_zone_by_commissioned_on = Window.partitionBy("zone").orderBy("commissioned_on", "id")
w_zone_tariff_by_installed_kw_3 = Window.partitionBy("zone", "tariff").orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_sites = (
    df_sites
    .filter((F.col("commissioned_on") >= F.lit("2024-10-28").cast("date")) & (F.length(F.trim(F.col("site_name"))) >= 8))
)

ranked = (
    valid_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        F.round(F.percent_rank().over(w_zone_tariff_by_installed_kw), 3).alias("installed_kw_percent_rank"),
        F.last("installed_kw", ignorenulls=True).over(w_zone_tariff_by_installed_kw_2).alias("last_known_installed_kw"),
        F.sum("installed_kw").over(w_zone_tariff).alias("zone_tariff_sum_installed_kw"),
        F.lag("commissioned_on").over(w_zone_by_commissioned_on).alias("prev_commissioned_on"),
        F.round(F.col("installed_kw") / F.nullif(F.sum("installed_kw").over(w_zone_tariff), F.lit(0)), 4).alias("installed_kw_share_of_zone_tariff"),
        F.first("commissioned_on").over(w_zone_tariff_by_installed_kw_3).alias("first_commissioned_on"),
    )
)

tagged = (
    ranked
    .select(
        "*",
        F.year(F.col("prev_commissioned_on")).alias("prev_commissioned_on_year"),
        F.when(F.col("tariff").isin("EV", "TOU"), "group_a").when(F.col("tariff") == "FLAT", "group_b").otherwise("other").alias("tariff_group"),
        F.dayofweek(F.col("commissioned_on")).alias("commissioned_on_dayofweek"),
    )
)

screened = (
    tagged.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left_semi")
)

with_children_readings = (
    screened.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.site_name",
        "s.zone",
        "s.commissioned_on",
        "s.installed_kw_percent_rank",
        "s.prev_commissioned_on",
        "s.installed_kw_share_of_zone_tariff",
        "s.prev_commissioned_on_year",
        "s.commissioned_on_dayofweek",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    with_children_readings
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        "prev_commissioned_on",
        "installed_kw_share_of_zone_tariff",
        "commissioned_on_dayofweek",
        "reading_id",
        "reading_kwh",
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
    )
    .orderBy(
        "prev_commissioned_on",
        F.col("site_name").asc(),
        "id",
        "reading_id",
    )
    .limit(20)
)
