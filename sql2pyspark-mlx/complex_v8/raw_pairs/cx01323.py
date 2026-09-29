from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_by_least_kwh_voltage = Window.partitionBy("site_id").orderBy("least_kwh_voltage", "id")
w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_least_kwh_voltage_quintile_b = Window.partitionBy("least_kwh_voltage_quintile").orderBy("read_at", "id")
w_least_kwh_voltage_quintile_b_2 = Window.partitionBy("least_kwh_voltage_quintile").orderBy("read_at")
w_all_by_quality_site_id_min_v = Window.orderBy("quality_site_id_min_voltage", "id").rowsBetween(-6, -1)

scoped = (
    df_meter_readings
    .filter((F.col("kwh") >= 26.9) & (F.col("voltage").between(213.5, 232.0)))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.round(F.col("least_kwh_voltage") - F.avg("least_kwh_voltage").over(w_quality_site_id), 2).alias("least_kwh_voltage_vs_quality_site_id_avg"),
        F.ntile(5).over(w_site_id_by_least_kwh_voltage).alias("least_kwh_voltage_quintile"),
        F.min("voltage").over(w_quality_site_id).alias("quality_site_id_min_voltage"),
        F.lead("least_kwh_voltage").over(w_site_id_by_least_kwh_voltage).alias("next_least_kwh_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
    )
)

joined_site = (
    with_windows.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").isNotNull()), "left")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

ranked = (
    joined_site
    .select(
        "*",
        F.count(F.lit(1)).over(w_quality_site_zone).alias("rows_in_quality_site_zone"),
        F.round(F.avg("rows_in_quality_site_id").over(w_quality_site_zone), 2).alias("quality_site_zone_avg_rows_in_quality_site_id"),
        F.dense_rank().over(w_least_kwh_voltage_quintile_b).alias("read_at_dense_rank"),
        F.min("voltage").over(w_quality_site_zone).alias("quality_site_zone_min_voltage"),
        F.round(F.cume_dist().over(w_least_kwh_voltage_quintile_b_2), 3).alias("read_at_cume_dist"),
        F.sum("voltage").over(w_all_by_quality_site_id_min_v).alias("voltage_sum_6_prior"),
    )
)

leaders = ranked.filter(F.col("read_at_dense_rank") <= 3)

result = (
    leaders
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "least_kwh_voltage",
        "rows_in_quality_site_id",
        "least_kwh_voltage_vs_quality_site_id_avg",
        "least_kwh_voltage_quintile",
        "quality_site_id_min_voltage",
        "next_least_kwh_voltage",
        "site_tariff",
        "site_zone",
        "site_commissioned_on",
        "rows_in_quality_site_zone",
        "quality_site_zone_avg_rows_in_quality_site_id",
        "read_at_dense_rank",
        "quality_site_zone_min_voltage",
        "read_at_cume_dist",
        F.abs(F.col("read_at_cume_dist") - 0.3).alias("read_at_cume_dist_distance"),
        F.concat(F.col("site_tariff"), F.lit("#"), F.col("read_at_dense_rank").cast("string")).alias("site_tariff_ref"),
    )
    .orderBy(
        F.col("site_zone").desc_nulls_first(),
        F.col("least_kwh_voltage_quintile").desc(),
    )
)
