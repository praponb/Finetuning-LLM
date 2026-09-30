from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_date_year_site_tarif = Window.partitionBy(F.year("read_at_date"), "site_tariff").orderBy(F.col("site_installed_kw_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_date_year_site_tarif_2 = Window.partitionBy(F.year("read_at_date"), "site_tariff").orderBy(F.col("site_installed_kw_2").desc(), "id")
w_site_commissioned_on_year = Window.partitionBy(F.year("site_commissioned_on"))

base_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(2, 3)) & (F.col("voltage") >= 222.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

enriched = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.greatest_kwh_voltage",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

labeled = (
    enriched
    .filter(F.col("greatest_kwh_voltage") <= 208.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "greatest_kwh_voltage",
        "site_installed_kw",
        "site_commissioned_on",
        F.date_format(F.col("site_commissioned_on"), "yyyy-MM").alias("site_commissioned_on_label"),
        (F.col("site_installed_kw") - F.col("kwh")).alias("site_installed_kw_minus_kwh"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

joined = (
    labeled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw_2"),
        F.col("si.zone").alias("site_zone"),
    )
)

windowed = (
    joined
    .select(
        "id",
        "read_at",
        "site_installed_kw",
        "site_commissioned_on_label",
        "site_installed_kw_minus_kwh",
        "read_at_date",
        "site_tariff",
        "site_installed_kw_2",
        "site_zone",
        F.max("site_installed_kw").over(w_read_at_date_year_site_tarif).alias("max_site_installed_kw_so_far"),
        F.rank().over(w_read_at_date_year_site_tarif_2).alias("site_installed_kw_2_rank"),
        F.round(F.col("site_installed_kw_minus_kwh") / F.nullif(F.sum("site_installed_kw_minus_kwh").over(w_site_commissioned_on_year), F.lit(0)), 4).alias("site_installed_kw_minus_kwh_share_of_site_commis"),
        F.round(F.col("greatest_kwh_voltage") / F.nullif(F.sum("greatest_kwh_voltage").over(w_site_commissioned_on_year), F.lit(0)), 4).alias("greatest_kwh_voltage_share_of_site_commissioned_"),
        F.round(F.avg("site_installed_kw_minus_kwh").over(w_site_commissioned_on_year), 2).alias("site_commissioned_on_year_avg_site_installed_kw_"),
        F.ntile(5).over(w_read_at_date_year_site_tarif_2).alias("site_installed_kw_2_quintile"),
    )
)

result = (
    windowed
    .select(
        "id",
        "read_at",
        "site_commissioned_on_label",
        "site_installed_kw_minus_kwh",
        "read_at_date",
        "site_tariff",
        "greatest_kwh_voltage_share_of_site_commissioned_",
        "site_commissioned_on_year_avg_site_installed_kw_",
        "site_installed_kw_2_quintile",
        F.concat(F.col("site_zone"), F.lit("#"), F.col("site_installed_kw_2_rank").cast("string")).alias("site_zone_ref"),
    )
)
