from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone = Window.partitionBy("site_zone")
w_site_zone_by_kwh = Window.partitionBy("site_zone").orderBy("kwh", "id")
w_site_tariff_site_zone = Window.partitionBy("site_tariff", "site_zone")
w_site_tariff_by_kwh = Window.partitionBy("site_tariff").orderBy("kwh", "id")
w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_month_by_vol = Window.partitionBy("site_id", F.month("read_at")).orderBy("voltage")
w_site_id_site_zone_by_read_at = Window.partitionBy("site_id", "site_zone").orderBy("read_at", "id")
w_site_tariff_read_at_year = Window.partitionBy("site_tariff", F.year("read_at"))
w_site_tariff_by_kwh_full = Window.partitionBy("site_tariff").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_read_at_month = Window.partitionBy(F.month("second_read_at"))
w_read_at_month_by_next2_volta = Window.partitionBy(F.month("read_at")).orderBy("next2_voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_by_site_installed = Window.partitionBy("site_zone").orderBy("site_installed_kw_share_of_site_tariff_site_zone", "id")
w_read_at_month_by_next2_volta_2 = Window.partitionBy(F.month("read_at")).orderBy("next2_voltage", "id").rowsBetween(-6, -1)

prepared_readings = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("site_id") >= 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

joined_site = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") >= F.lit("2024-08-24").cast("date")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_ref",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

windowed = (
    joined_site
    .select(
        "*",
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_zone), F.lit(0)), 4).alias("site_installed_kw_share_of_site_zone"),
        F.lag("kwh", 3).over(w_site_zone_by_kwh).alias("prev3_kwh"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_tariff_site_zone), F.lit(0)), 4).alias("site_installed_kw_share_of_site_tariff_site_zone"),
        F.lead("voltage").over(w_site_tariff_by_kwh).alias("next_voltage"),
        F.sum("voltage").over(w_site_tariff_site_zone).alias("site_tariff_site_zone_sum_voltage"),
        F.first("voltage").over(w_site_id_by_read_at_full).alias("first_voltage"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_site_tariff_site_zone), 2).alias("site_installed_kw_vs_site_tariff_site_zone_avg"),
        F.lead("voltage", 2).over(w_site_tariff_by_kwh).alias("next2_voltage"),
        F.max("site_installed_kw").over(w_site_zone).alias("site_zone_max_site_installed_kw"),
        F.rank().over(w_site_id_read_at_month_by_vol).alias("voltage_rank"),
        F.lag("site_installed_kw").over(w_site_id_site_zone_by_read_at).alias("prev_site_installed_kw"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_tariff_read_at_year), 2).alias("kwh_vs_site_tariff_read_at_year_avg"),
        F.nth_value("read_at", 2).over(w_site_tariff_by_kwh_full).alias("second_read_at"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_site_zone_by_read_at)).alias("voltage_change"),
        F.sum("voltage").over(w_site_zone).alias("site_zone_sum_voltage"),
    )
)

leaders = windowed.filter(F.col("voltage_rank") == 1)

windowed_2 = (
    leaders
    .select(
        "id",
        "kwh",
        "quality_ref",
        "site_zone",
        "site_installed_kw_share_of_site_tariff_site_zone",
        "next_voltage",
        "site_tariff_site_zone_sum_voltage",
        "first_voltage",
        "site_installed_kw_vs_site_tariff_site_zone_avg",
        "next2_voltage",
        "site_zone_max_site_installed_kw",
        "voltage_rank",
        "kwh_vs_site_tariff_read_at_year_avg",
        "second_read_at",
        F.round(F.col("site_zone_sum_voltage") / F.nullif(F.sum("site_zone_sum_voltage").over(w_second_read_at_month), F.lit(0)), 4).alias("site_zone_sum_voltage_share_of_second_read_at_mo"),
        F.round(F.col("voltage_change") / F.nullif(F.sum("voltage_change").over(w_second_read_at_month), F.lit(0)), 4).alias("voltage_change_share_of_second_read_at_month"),
        F.first("voltage").over(w_read_at_month_by_next2_volta).alias("first_voltage_2"),
        (F.col("prev_site_installed_kw") - F.lag("prev_site_installed_kw").over(w_site_zone_by_site_installed)).alias("prev_site_installed_kw_change"),
        F.round(F.col("prev_site_installed_kw") / F.nullif(F.sum("prev_site_installed_kw").over(w_second_read_at_month), F.lit(0)), 4).alias("prev_site_installed_kw_share_of_second_read_at_m"),
        F.round(F.avg("next_voltage").over(w_read_at_month_by_next2_volta_2), 2).alias("next_voltage_avg_6_prior"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_second_read_at_month), F.lit(0)), 4).alias("voltage_share_of_second_read_at_month"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "kwh",
        "site_zone",
        "site_installed_kw_share_of_site_tariff_site_zone",
        "next_voltage",
        "site_tariff_site_zone_sum_voltage",
        "site_installed_kw_vs_site_tariff_site_zone_avg",
        "next2_voltage",
        "site_zone_max_site_installed_kw",
        "voltage_rank",
        "kwh_vs_site_tariff_read_at_year_avg",
        "site_zone_sum_voltage_share_of_second_read_at_mo",
        "first_voltage_2",
        "prev_site_installed_kw_change",
        "prev_site_installed_kw_share_of_second_read_at_m",
        "voltage_share_of_second_read_at_month",
        F.lower(F.regexp_replace(F.trim(F.col("site_zone")), " ", "_")).alias("site_zone_slug"),
        F.when(F.col("voltage_rank") >= 7, "large").when(F.col("voltage_rank") >= 4, "medium").otherwise("small").alias("voltage_rank_tier"),
    )
    .orderBy(F.col("site_tariff_site_zone_sum_voltage").desc())
)
