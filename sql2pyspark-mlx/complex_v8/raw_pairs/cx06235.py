from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_voltage_tier_site_id_by_site = Window.partitionBy("voltage_tier", "site_id").orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_distance = Window.orderBy("voltage_distance", "id")
w_quality_voltage_tier_by_site = Window.partitionBy("quality", "voltage_tier").orderBy("site_commissioned_on", "id")
w_quality_voltage_tier_by_site_2 = Window.partitionBy("quality", "voltage_tier").orderBy("site_commissioned_on")
w_site_tariff_quality = Window.partitionBy("site_tariff", "quality")
w_all_by_voltage_distance_movi = Window.orderBy("voltage_distance", "id").rowsBetween(-1, Window.currentRow)
w_all_by_voltage_distance_ties = Window.orderBy("voltage_distance")
w_voltage_tier_site_tariff = Window.partitionBy("voltage_tier", "site_tariff")
w_site_commissioned_on_dense_r = Window.partitionBy("site_commissioned_on_dense_rank", "voltage_tier").orderBy("site_commissioned_on")
w_site_commissioned_on_dense_r_2 = Window.partitionBy("site_commissioned_on_dense_rank", F.month("site_commissioned_on")).orderBy("voltage_distance_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_commissioned_on_dense_r_3 = Window.partitionBy("site_commissioned_on_dense_rank", F.month("site_commissioned_on")).orderBy("voltage_distance_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_zone_by_site_co = Window.partitionBy("quality", "site_zone").orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 237.5, "large").when(F.col("voltage") >= 222.6, "medium").otherwise("small").alias("voltage_tier"),
        F.abs(F.col("voltage") - 222.1).alias("voltage_distance"),
    )
)

enriched_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

windowed = (
    enriched_site
    .select(
        "*",
        F.nth_value("site_commissioned_on", 2).over(w_voltage_tier_site_id_by_site).alias("second_site_commissioned_on"),
        F.round(F.cume_dist().over(w_all_by_voltage_distance), 3).alias("voltage_distance_cume_dist"),
        F.lead("kwh", 1, 0).over(w_quality_voltage_tier_by_site).alias("next_kwh"),
        F.dense_rank().over(w_quality_voltage_tier_by_site_2).alias("site_commissioned_on_dense_rank"),
        F.count(F.lit(1)).over(w_site_tariff_quality).alias("rows_in_site_tariff_quality"),
        F.sum("voltage").over(w_site_tariff_quality).alias("site_tariff_quality_sum_voltage"),
        F.sum("voltage_distance").over(w_all_by_voltage_distance_movi).alias("voltage_distance_sum_2"),
        F.dense_rank().over(w_all_by_voltage_distance_ties).alias("voltage_distance_dense_rank"),
    )
)

top_rows = windowed.filter(F.col("site_commissioned_on_dense_rank") <= 2)

with_trends = (
    top_rows
    .select(
        "*",
        F.count(F.lit(1)).over(w_voltage_tier_site_tariff).alias("rows_in_voltage_tier_site_tariff"),
        F.rank().over(w_site_commissioned_on_dense_r).alias("site_commissioned_on_rank"),
        F.first("next_kwh", ignorenulls=True).over(w_site_commissioned_on_dense_r_2).alias("first_known_next_kwh"),
        F.last("voltage_distance").over(w_site_commissioned_on_dense_r_3).alias("last_voltage_distance"),
        F.round(F.col("voltage_distance_cume_dist") - F.avg("voltage_distance_cume_dist").over(w_voltage_tier_site_tariff), 2).alias("voltage_distance_cume_dist_vs_voltage_tier_site_"),
        F.count(F.lit(1)).over(w_quality_site_zone_by_site_co).alias("running_count"),
        F.max("voltage").over(w_voltage_tier_site_tariff).alias("voltage_tier_site_tariff_max_voltage"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "kwh",
        "voltage_distance",
        "site_zone",
        "site_tariff",
        "second_site_commissioned_on",
        "voltage_distance_cume_dist",
        "site_commissioned_on_dense_rank",
        "rows_in_site_tariff_quality",
        "site_tariff_quality_sum_voltage",
        "voltage_distance_sum_2",
        "site_commissioned_on_rank",
        "last_voltage_distance",
        "voltage_distance_cume_dist_vs_voltage_tier_site_",
        "running_count",
        "voltage_tier_site_tariff_max_voltage",
    )
    .orderBy(F.col("site_zone").asc(), "site_tariff")
)
