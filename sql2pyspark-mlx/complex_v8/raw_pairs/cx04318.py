from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_quality_by_kwh_fil = Window.partitionBy("site_zone", "quality").orderBy(F.col("kwh_filled").desc())
w_site_commissioned_on_year_si = Window.partitionBy(F.year("site_commissioned_on"), "site_zone").orderBy(F.col("voltage").desc())
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_zone_quality_by_kwh_fil_2 = Window.partitionBy("site_zone", "quality").orderBy(F.col("kwh_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_quality_by_kwh_fil_3 = Window.partitionBy("site_zone", "quality").orderBy(F.col("kwh_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_quality_by_kwh_fil_4 = Window.partitionBy("site_zone", "quality").orderBy(F.col("kwh_filled").desc(), "id").rowsBetween(-1, Window.currentRow)
w_site_zone_quality_by_kwh_fil_5 = Window.partitionBy("site_zone", "quality").orderBy(F.col("kwh_filled").desc(), "id")
w_quality_site_zone_by_kwh_ful = Window.partitionBy("quality", "site_zone").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_commissioned_on_year_si_2 = Window.partitionBy(F.year("site_commissioned_on"), "site_zone").orderBy(F.col("voltage").desc(), "id")
w_quality_site_zone_by_kwh = Window.partitionBy("quality", "site_zone").orderBy("kwh", "id")
w_all_by_last_known_read_at_ti = Window.orderBy(F.col("last_known_read_at").desc())
w_all_by_last_known_read_at_fu = Window.orderBy(F.col("last_known_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_known_read_at_month = Window.partitionBy(F.date_trunc("month", "last_known_read_at"))
w_all_by_last_known_read_at_mo = Window.orderBy(F.col("last_known_read_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_site_zone_kwh_filled_rank_by = Window.partitionBy("site_zone", "kwh_filled_rank").orderBy("site_id_quality_max_kwh", "id")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

with_parent_site = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

metrics = (
    with_parent_site
    .select(
        "*",
        F.rank().over(w_site_zone_quality_by_kwh_fil).alias("kwh_filled_rank"),
        F.round(F.percent_rank().over(w_site_commissioned_on_year_si), 3).alias("voltage_percent_rank"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_id_quality), F.lit(0)), 4).alias("site_installed_kw_share_of_site_id_quality"),
        F.first("site_installed_kw").over(w_site_zone_quality_by_kwh_fil_2).alias("first_site_installed_kw"),
        F.max("site_installed_kw").over(w_site_zone_quality_by_kwh_fil_3).alias("max_site_installed_kw_so_far"),
        F.round(F.avg("kwh_filled").over(w_site_zone_quality_by_kwh_fil_4), 2).alias("kwh_filled_avg_2"),
        F.row_number().over(w_site_zone_quality_by_kwh_fil_5).alias("kwh_filled_seq"),
        F.first("voltage").over(w_quality_site_zone_by_kwh_ful).alias("first_voltage"),
        F.max("kwh").over(w_site_id_quality).alias("site_id_quality_max_kwh"),
        (F.col("site_installed_kw") - F.lag("site_installed_kw").over(w_site_commissioned_on_year_si_2)).alias("site_installed_kw_change"),
        F.max("kwh_filled").over(w_site_id_quality).alias("site_id_quality_max_kwh_filled"),
        F.lag("kwh").over(w_quality_site_zone_by_kwh).alias("prev_kwh"),
        F.sum("voltage").over(w_site_id_quality).alias("site_id_quality_sum_voltage"),
        F.last("read_at", ignorenulls=True).over(w_site_zone_quality_by_kwh_fil_3).alias("last_known_read_at"),
        F.lag("read_at").over(w_site_commissioned_on_year_si_2).alias("prev_read_at"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_commissioned_on",
        "site_installed_kw",
        "site_zone",
        "kwh_filled_rank",
        "voltage_percent_rank",
        "site_installed_kw_share_of_site_id_quality",
        "kwh_filled_avg_2",
        "kwh_filled_seq",
        "first_voltage",
        "site_installed_kw_change",
        "site_id_quality_max_kwh_filled",
        "prev_kwh",
        "site_id_quality_sum_voltage",
        "last_known_read_at",
        F.round(F.cume_dist().over(w_all_by_last_known_read_at_ti), 3).alias("last_known_read_at_cume_dist"),
        F.nth_value("site_id_quality_max_kwh", 2).over(w_all_by_last_known_read_at_fu).alias("second_site_id_quality_max_kwh"),
        F.round(F.col("first_site_installed_kw") / F.nullif(F.sum("first_site_installed_kw").over(w_last_known_read_at_month), F.lit(0)), 4).alias("first_site_installed_kw_share_of_last_known_read"),
        F.max("site_installed_kw").over(w_all_by_last_known_read_at_mo).alias("site_installed_kw_max_4"),
        F.lag("read_at").over(w_site_zone_kwh_filled_rank_by).alias("prev_read_at_2"),
    )
)

result = (
    windowed
    .filter(F.col("kwh_filled_avg_2") <= 72.0)
    .select(
        "id",
        "read_at",
        "kwh",
        "site_commissioned_on",
        "site_zone",
        "kwh_filled_rank",
        "kwh_filled_avg_2",
        "kwh_filled_seq",
        "first_voltage",
        "site_installed_kw_change",
        "last_known_read_at",
        "last_known_read_at_cume_dist",
        "second_site_id_quality_max_kwh",
        "first_site_installed_kw_share_of_last_known_read",
        "prev_read_at_2",
        (F.floor(F.col("prev_kwh") / 25) * 25).alias("prev_kwh_bucket"),
    )
)
