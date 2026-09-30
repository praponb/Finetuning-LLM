from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone = Window.partitionBy("site_zone")
w_site_tariff_by_kwh_ties = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc())
w_site_commissioned_on_year_si = Window.partitionBy(F.year("site_commissioned_on"), "site_tariff")
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc_nulls_first(), "id")
w_site_tariff_site_id_by_volta = Window.partitionBy("site_tariff", "site_id").orderBy(F.col("voltage").desc())
w_site_tariff_by_kwh_full = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_tariff_site_commissione = Window.partitionBy("site_tariff", F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("site_commissioned_on").desc())
w_all_by_site_commissioned_on_2 = Window.orderBy(F.col("site_commissioned_on").desc_nulls_first())
w_site_tariff_by_kwh = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc(), "id")
w_site_tariff_site_id_by_volta_2 = Window.partitionBy("site_tariff", "site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(-3, -1)
w_site_tariff_by_kwh_running = Window.partitionBy("site_tariff").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_readings = df_meter_readings.filter(F.col("kwh") >= 30.2)

with_parent_site = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 141.6), "inner")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

sequenced = (
    with_parent_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_tariff",
        "site_commissioned_on",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_zone), F.lit(0)), 4).alias("voltage_share_of_site_zone"),
        F.round(F.percent_rank().over(w_site_tariff_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.sum("voltage").over(w_site_commissioned_on_year_si).alias("site_commissioned_on_year_site_tariff_sum_voltag"),
        F.round(F.cume_dist().over(w_all_by_site_commissioned_on), 3).alias("site_commissioned_on_cume_dist"),
        F.round(F.percent_rank().over(w_site_tariff_site_id_by_volta), 3).alias("voltage_percent_rank"),
        F.nth_value("voltage", 2).over(w_site_tariff_by_kwh_full).alias("second_voltage"),
        F.sum("kwh").over(w_site_zone).alias("site_zone_sum_kwh"),
        F.min("voltage").over(w_site_zone).alias("site_zone_min_voltage"),
        F.round(F.cume_dist().over(w_site_tariff_site_commissione), 3).alias("site_commissioned_on_cume_dist_2"),
        F.round(F.cume_dist().over(w_all_by_site_commissioned_on_2), 3).alias("site_commissioned_on_cume_dist_3"),
        (F.col("kwh") - F.lag("kwh").over(w_site_tariff_by_kwh)).alias("kwh_change"),
        F.round(F.avg("voltage").over(w_site_tariff_site_id_by_volta_2), 2).alias("voltage_avg_3_prior"),
        F.last("voltage", ignorenulls=True).over(w_site_tariff_by_kwh_running).alias("last_known_voltage"),
    )
)

tagged = (
    sequenced
    .filter(F.col("site_tariff").isNotNull())
    .select(
        "*",
        F.initcap(F.trim(F.col("site_tariff"))).alias("site_tariff_clean"),
        F.round(F.col("site_zone_sum_kwh") / F.nullif(F.col("last_known_voltage"), F.lit(0)), 3).alias("site_zone_sum_kwh_per_last_known_voltage"),
    )
)

totals = (
    tagged
    .groupBy(
        F.year("site_commissioned_on").alias("site_commissioned_on_year"),
        "quality",
    )
    .agg(
        F.sum(F.when(~F.col("site_tariff").isin("TOU", "FLAT"), 1).otherwise(0)).alias("n_site_tariff_not_in_tou_flat"),
        F.count("*").alias("n_rows"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.round(F.sum("site_zone_min_voltage") / F.nullif(F.sum("site_commissioned_on_cume_dist_3"), F.lit(0)), 3).alias("site_zone_min_voltage_to_site_commissioned_on_cu"),
        F.sum("voltage_percent_rank").alias("total_voltage_percent_rank"),
        F.max("site_commissioned_on_cume_dist_2").alias("max_site_commissioned_on_cume_dist_2"),
    )
)

result = (
    totals
    .select(
        "site_commissioned_on_year",
        "n_site_tariff_not_in_tou_flat",
        "distinct_qualitys",
        "site_zone_min_voltage_to_site_commissioned_on_cu",
        "max_site_commissioned_on_cume_dist_2",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)
