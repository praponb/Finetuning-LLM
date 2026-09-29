from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_read_at_month_by = Window.partitionBy("site_tariff", F.date_trunc("month", "read_at")).orderBy(F.col("read_at").desc_nulls_first(), "id")
w_site_zone = Window.partitionBy("site_zone")
w_all_by_site_installed_kw_run = Window.orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at = Window.orderBy("read_at", "id")
w_all_by_site_installed_kw_pri = Window.orderBy("site_installed_kw", "id").rowsBetween(-3, -1)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_tariff = Window.partitionBy("site_tariff")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_all_by_voltage_moving_3 = Window.orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter(F.col("voltage").between(220.2, 241.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

combined_details = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") == "TOU")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.quality_slug",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "site_tariff",
        "site_zone",
        "site_installed_kw",
        F.lag("site_installed_kw", 3).over(w_site_tariff_read_at_month_by).alias("prev3_site_installed_kw"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_zone), F.lit(0)), 4).alias("site_installed_kw_share_of_site_zone"),
        F.min("site_installed_kw").over(w_all_by_site_installed_kw_run).alias("min_site_installed_kw_so_far"),
        F.first("read_at", ignorenulls=True).over(w_all_by_site_installed_kw_run).alias("first_known_read_at"),
        F.row_number().over(w_all_by_read_at).alias("read_at_seq"),
        F.round(F.avg("site_installed_kw").over(w_all_by_site_installed_kw_pri), 2).alias("site_installed_kw_avg_3_prior"),
        F.ntile(5).over(w_all_by_read_at).alias("read_at_quintile"),
        F.max("voltage").over(w_quality_site_id).alias("quality_site_id_max_voltage"),
        F.ntile(4).over(w_site_tariff_read_at_month_by).alias("read_at_quartile"),
        F.sum("site_installed_kw").over(w_site_tariff).alias("site_tariff_sum_site_installed_kw"),
        F.round(F.avg("voltage").over(w_quality_site_id), 2).alias("quality_site_id_avg_voltage"),
        F.row_number().over(w_all_by_voltage).alias("rn"),
        F.round(F.avg("site_installed_kw").over(w_all_by_voltage_moving_3), 2).alias("site_installed_kw_avg_4"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
    )
)

first_rows = (
    with_trends
    .filter((F.col("read_at_seq") == 1) & (F.col("quality_site_id_avg_voltage").between(218.8, 231.0)))
)

result = (
    first_rows
    .filter(F.col("site_installed_kw") >= 96.9)
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "site_tariff",
        "min_site_installed_kw_so_far",
        "first_known_read_at",
        "read_at_seq",
        "site_installed_kw_avg_3_prior",
        "read_at_quintile",
        "read_at_quartile",
        "site_tariff_sum_site_installed_kw",
        "quality_site_id_avg_voltage",
        "rn",
        "site_installed_kw_avg_4",
        "rows_in_quality_site_id",
        F.concat_ws(": ", F.col("site_zone"), F.col("site_tariff")).alias("site_zone_site_tariff_label"),
    )
    .orderBy(
        "read_at_quartile",
        F.col("site_tariff_sum_site_installed_kw").desc_nulls_first(),
    )
)
