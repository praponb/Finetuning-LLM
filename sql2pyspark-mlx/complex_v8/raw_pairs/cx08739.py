from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_zone_by_kwh_dis = Window.partitionBy("quality", "site_zone").orderBy("kwh_distance", "id").rowsBetween(-2, Window.currentRow)
w_quality_site_zone_by_kwh_dis_2 = Window.partitionBy("quality", "site_zone").orderBy("kwh_distance", "id")
w_site_tariff_by_kwh = Window.partitionBy("site_tariff").orderBy("kwh", "id")
w_site_zone_by_kwh_ties = Window.partitionBy("site_zone").orderBy(F.col("kwh").desc())
w_site_tariff_site_zone = Window.partitionBy("site_tariff", "site_zone")
w_site_zone_by_kwh_full = Window.partitionBy("site_zone").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_zone_by_kwh_dis_3 = Window.partitionBy("quality", "site_zone").orderBy("kwh_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_tariff_by_kwh_moving_3 = Window.partitionBy("site_tariff").orderBy("kwh", "id").rowsBetween(-3, Window.currentRow)
w_site_zone_by_kwh_running = Window.partitionBy("site_zone").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_by_kwh_prior_3 = Window.partitionBy("site_tariff").orderBy("kwh", "id").rowsBetween(-3, -1)

base_readings = (
    df_meter_readings
    .filter((F.col("site_id") <= 6) & (F.col("kwh") >= 24.5))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 51.5).alias("kwh_distance"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

enriched_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

metrics = (
    enriched_site
    .select(
        "*",
        F.max("kwh_distance").over(w_quality_site_zone_by_kwh_dis).alias("kwh_distance_max_3"),
        F.lag("kwh", 3).over(w_quality_site_zone_by_kwh_dis_2).alias("prev3_kwh"),
        F.lead("voltage_bucket", 2, 0).over(w_site_tariff_by_kwh).alias("next2_voltage_bucket"),
        F.round(F.cume_dist().over(w_site_zone_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.round(F.avg("voltage_bucket").over(w_site_tariff_site_zone), 2).alias("site_tariff_site_zone_avg_voltage_bucket"),
        F.round(F.percent_rank().over(w_site_zone_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.last("kwh_distance").over(w_site_zone_by_kwh_full).alias("last_kwh_distance"),
        F.round(F.col("voltage_bucket") - F.avg("voltage_bucket").over(w_site_tariff_site_zone), 2).alias("voltage_bucket_vs_site_tariff_site_zone_avg"),
        F.nth_value("kwh_distance", 2).over(w_quality_site_zone_by_kwh_dis_3).alias("second_kwh_distance"),
        F.sum("kwh_distance").over(w_site_tariff_by_kwh_moving_3).alias("kwh_distance_sum_4"),
        F.max("voltage").over(w_site_zone_by_kwh_running).alias("max_voltage_so_far"),
        F.count(F.lit(1)).over(w_site_tariff_site_zone).alias("rows_in_site_tariff_site_zone"),
        F.max("voltage_bucket").over(w_site_tariff_by_kwh_prior_3).alias("voltage_bucket_max_3_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "voltage",
        "quality",
        "kwh_distance",
        "voltage_bucket",
        "site_tariff",
        "kwh_distance_max_3",
        "prev3_kwh",
        "next2_voltage_bucket",
        "kwh_cume_dist",
        "site_tariff_site_zone_avg_voltage_bucket",
        "kwh_percent_rank",
        "last_kwh_distance",
        "voltage_bucket_vs_site_tariff_site_zone_avg",
        "kwh_distance_sum_4",
        "voltage_bucket_max_3_prior",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
    .orderBy(F.col("voltage_bucket_max_3_prior").asc())
)
