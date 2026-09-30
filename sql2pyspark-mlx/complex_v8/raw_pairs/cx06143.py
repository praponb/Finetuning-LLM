from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on_2").desc_nulls_first(), "id")
w_site_id = Window.partitionBy("site_id")

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.abs(F.col("kwh") - 81.9).alias("kwh_distance"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

filtered = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 141.6) & (F.col("si.tariff").isNotNull()), "left_anti")
)

joined_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") < 284.0), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.kwh_distance",
        "s.read_at_year",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

enriched_site = (
    joined_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.kwh_distance",
        "s.site_name",
        "s.site_commissioned_on",
        F.col("si.tariff").alias("site_tariff"),
    )
)

joined = (
    enriched_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "TOU"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.kwh_distance",
        "s.site_name",
        "s.site_tariff",
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
        F.col("si.site_name").alias("site_site_name"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "kwh_distance",
        "site_name",
        "site_tariff",
        "site_commissioned_on_2",
        F.lead("kwh_distance", 3, 0).over(w_all_by_site_commissioned_on).alias("next3_kwh_distance"),
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
    )
)

result = (
    metrics
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "kwh_distance",
        "site_name",
        "site_commissioned_on_2",
        "next3_kwh_distance",
        "site_id_min_voltage",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)
