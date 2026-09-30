from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month = Window.partitionBy(F.month("read_at"))
w_read_at_month_site_id = Window.partitionBy(F.month("read_at"), "site_id")
w_read_at_year_site_tariff_by = Window.partitionBy(F.year("read_at"), "site_tariff").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_tariff_quality_by_kwh = Window.partitionBy("site_tariff", "quality").orderBy(F.col("kwh").desc(), "id")
w_site_tariff_quality_by_kwh_r = Window.partitionBy("site_tariff", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_site_tariff_by_2 = Window.partitionBy(F.year("read_at"), "site_tariff").orderBy("kwh", "id")

valid_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 106.0) | (F.col("site_id") <= 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

enriched = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") != "TOU"), "left")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_read_at_month).alias("rows_in_read_at_month"),
        F.round(F.avg("kwh").over(w_read_at_month_site_id), 2).alias("read_at_month_site_id_avg_kwh"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_month_site_id), F.lit(0)), 4).alias("voltage_share_of_read_at_month_site_id"),
        F.sum("least_kwh_voltage").over(w_read_at_month_site_id).alias("read_at_month_site_id_sum_least_kwh_voltage"),
        F.nth_value("read_at", 2).over(w_read_at_year_site_tariff_by).alias("second_read_at"),
        F.ntile(10).over(w_site_tariff_quality_by_kwh).alias("kwh_decile"),
        F.last("voltage", ignorenulls=True).over(w_site_tariff_quality_by_kwh_r).alias("last_known_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_month_site_id), 2).alias("voltage_vs_read_at_month_site_id_avg"),
        F.round(F.cume_dist().over(w_read_at_year_site_tariff_by_2), 3).alias("kwh_cume_dist"),
        F.count(F.lit(1)).over(w_read_at_month_site_id).alias("rows_in_read_at_month_site_id"),
    )
)

enriched_2 = (
    with_windows
    .select(
        "*",
        F.abs(F.col("kwh_decile") - 5).alias("kwh_decile_distance"),
        F.date_format(F.col("second_read_at"), "yyyy-MM-dd").alias("second_read_at_label"),
        F.concat_ws("-", F.col("site_zone"), F.col("quality")).alias("site_zone_quality_label"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_or_unknown",
        "least_kwh_voltage",
        "site_tariff",
        "site_zone",
        "rows_in_read_at_month",
        "read_at_month_site_id_sum_least_kwh_voltage",
        "second_read_at",
        "kwh_decile",
        "last_known_voltage",
        "voltage_vs_read_at_month_site_id_avg",
        "kwh_cume_dist",
        "rows_in_read_at_month_site_id",
        "kwh_decile_distance",
        "site_zone_quality_label",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
    .orderBy(F.col("site_zone_quality_label").asc(), "id")
    .limit(20)
)
