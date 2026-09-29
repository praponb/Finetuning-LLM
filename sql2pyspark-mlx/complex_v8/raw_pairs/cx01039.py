from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_quality_site_id_by_kwh_full = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_site_tariff_by_site = Window.partitionBy("site_id", "site_tariff").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_site_tariff_by_site_2 = Window.partitionBy("site_id", "site_tariff").orderBy("site_installed_kw", "id").rowsBetween(-6, Window.currentRow)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_quality_site_id_by_kwh_prior = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(-3, -1)
w_site_id_site_tariff_by_site_3 = Window.partitionBy("site_id", "site_tariff").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_site_tariff_by_site_4 = Window.partitionBy("site_id", "site_tariff").orderBy("site_installed_kw", "id").rowsBetween(-3, Window.currentRow)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_site_installed_kw_sha = Window.orderBy(F.col("site_installed_kw_share_of_quality").asc_nulls_last(), "id")

base = (
    df_meter_readings
    .filter((~F.col("quality").isin("OK", "EST")) & (F.hour(F.col("read_at")).between(9, 15)))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

sequenced = (
    with_parent
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
        F.first("voltage").over(w_quality_site_id_by_kwh_full).alias("first_voltage"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_quality), F.lit(0)), 4).alias("site_installed_kw_share_of_quality"),
        F.min("voltage").over(w_site_id_site_tariff_by_site).alias("min_voltage_so_far"),
        F.max("site_installed_kw").over(w_site_id_site_tariff_by_site_2).alias("site_installed_kw_max_7"),
        F.first("kwh", ignorenulls=True).over(w_site_id_site_tariff_by_site).alias("first_known_kwh"),
        F.round(F.cume_dist().over(w_all_by_kwh), 3).alias("kwh_cume_dist"),
        F.max("voltage").over(w_quality_site_id_by_kwh_prior).alias("voltage_max_3_prior"),
        F.count(F.lit(1)).over(w_site_id_site_tariff_by_site).alias("running_count"),
        F.sum("voltage").over(w_quality).alias("quality_sum_voltage"),
        F.last("site_installed_kw").over(w_site_id_site_tariff_by_site_3).alias("last_site_installed_kw"),
        F.sum("site_installed_kw").over(w_site_id_site_tariff_by_site_4).alias("site_installed_kw_sum_4"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "site_id",
        "quality",
        "site_name",
        "voltage_vs_quality_avg",
        "first_voltage",
        "site_installed_kw_share_of_quality",
        "min_voltage_so_far",
        "site_installed_kw_max_7",
        "first_known_kwh",
        "kwh_cume_dist",
        "running_count",
        "quality_sum_voltage",
        "last_site_installed_kw",
        F.round(F.avg("kwh_cume_dist").over(w_site_id_quality), 2).alias("site_id_quality_avg_kwh_cume_dist"),
        F.lead("kwh").over(w_all_by_site_installed_kw_sha).alias("next_kwh"),
        F.size(F.collect_set("site_tariff").over(w_site_id_quality)).alias("distinct_site_tariff_in_site_id_quality"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "site_id",
        "quality",
        "voltage_vs_quality_avg",
        "min_voltage_so_far",
        "site_installed_kw_max_7",
        "running_count",
        "site_id_quality_avg_kwh_cume_dist",
        "next_kwh",
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
        F.coalesce(F.col("first_voltage"), F.lit(0.0)).alias("first_voltage_filled"),
    )
    .orderBy(F.col("quality").asc_nulls_last(), "id")
    .limit(20)
)
