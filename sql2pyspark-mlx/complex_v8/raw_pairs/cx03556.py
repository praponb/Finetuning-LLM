from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month_tariff = Window.partitionBy(F.date_trunc("month", "commissioned_on"), "tariff").orderBy(F.col("commissioned_on").desc(), "id")
w_tariff_group = Window.partitionBy("tariff_group")

valid_sites = (
    df_sites
    .filter((F.col("installed_kw") > 141.6) & (F.col("zone").isin("Z2", "Z4")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

with_activity = (
    valid_sites.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh") < 113.0) & (F.col("m.quality").isin("OK", "EST")), "left_semi")
)

derived = (
    with_activity
    .filter(F.col("tariff").isin("TOU", "EV"))
    .withColumn("tariff_group", F.when(F.col("tariff").isin("FLAT", "TOU"), "group_a").when(F.col("tariff") == "EV", "group_b").otherwise("other"))
)

sequenced = (
    derived
    .select(
        "*",
        F.row_number().over(w_commissioned_on_month_tariff).alias("rn"),
        F.round(F.avg("installed_kw").over(w_tariff_group), 2).alias("tariff_group_avg_installed_kw"),
        F.max("installed_kw").over(w_tariff_group).alias("tariff_group_max_installed_kw"),
        F.min("installed_kw").over(w_tariff_group).alias("tariff_group_min_installed_kw"),
    )
)

line_level_readings = (
    sequenced.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.zone",
        "s.tariff",
        "s.rn",
        "s.tariff_group_avg_installed_kw",
        "s.tariff_group_min_installed_kw",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

screened = (
    line_level_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.voltage") > 214.3), "left_anti")
)

result = (
    screened
    .cube("reading_quality", "tariff", "rn")
    .agg(F.min("reading_kwh").alias("min_reading_kwh"))
    .orderBy(
        F.col("reading_quality").asc_nulls_last(),
        F.col("tariff").asc_nulls_last(),
        F.col("rn").asc_nulls_last(),
    )
)
