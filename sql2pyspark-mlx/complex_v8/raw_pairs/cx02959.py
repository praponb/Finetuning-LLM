from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_site_installed_kw = Window.partitionBy("site_id").orderBy("site_installed_kw", "id")
w_site_id_read_at_month_by_sit = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_site_id_by_read = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("read_at").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_read_at_year_site_id_by_read_2 = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("read_at").desc())
w_site_id_read_at_month_by_sit_2 = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy("site_installed_kw", "id")
w_read_at_year_site_id_by_read_3 = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_rank_by_site_install = Window.partitionBy("read_at_rank").orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_rank_by_site_install_2 = Window.partitionBy("read_at_rank").orderBy(F.col("site_installed_kw").desc())
w_read_at_rank = Window.partitionBy("read_at_rank")
w_read_at_rank_by_site_install_3 = Window.partitionBy("read_at_rank").orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter(F.col("kwh") >= 35.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_parent = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "read_at",
        "kwh",
        "site_installed_kw",
        F.lag("read_at").over(w_site_id_by_site_installed_kw).alias("prev_read_at"),
        F.last("read_at", ignorenulls=True).over(w_site_id_read_at_month_by_sit).alias("last_known_read_at"),
        F.lead("site_installed_kw").over(w_read_at_year_site_id_by_read).alias("next_site_installed_kw"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_site_id), F.lit(0)), 4).alias("site_installed_kw_share_of_site_id"),
        F.rank().over(w_read_at_year_site_id_by_read_2).alias("read_at_rank"),
        F.lag("kwh").over(w_site_id_by_site_installed_kw).alias("prev_kwh"),
        (F.col("kwh") - F.lag("kwh").over(w_site_id_by_site_installed_kw)).alias("kwh_change"),
        F.lag("site_installed_kw", 3).over(w_site_id_read_at_month_by_sit_2).alias("prev3_site_installed_kw"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.nth_value("site_installed_kw", 2).over(w_read_at_year_site_id_by_read_3).alias("second_site_installed_kw"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "kwh",
        "site_installed_kw",
        "prev_read_at",
        "last_known_read_at",
        "next_site_installed_kw",
        "site_installed_kw_share_of_site_id",
        "read_at_rank",
        "prev_kwh",
        "kwh_change",
        "second_site_installed_kw",
        F.nth_value("prev3_site_installed_kw", 2).over(w_read_at_rank_by_site_install).alias("second_prev3_site_installed_kw"),
        F.round(F.percent_rank().over(w_read_at_rank_by_site_install_2), 3).alias("site_installed_kw_percent_rank"),
        F.round(F.avg("kwh").over(w_read_at_rank), 2).alias("read_at_rank_avg_kwh"),
        F.count(F.lit(1)).over(w_read_at_rank_by_site_install_3).alias("running_count"),
        F.round(F.cume_dist().over(w_read_at_rank_by_site_install_2), 3).alias("site_installed_kw_cume_dist"),
        F.count(F.lit(1)).over(w_read_at_rank).alias("rows_in_read_at_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_installed_kw",
        "prev_read_at",
        "next_site_installed_kw",
        "read_at_rank",
        "prev_kwh",
        "kwh_change",
        "second_site_installed_kw",
        "read_at_rank_avg_kwh",
        "site_installed_kw_cume_dist",
        "rows_in_read_at_rank",
        (F.floor(F.col("kwh_change") / 50) * 50).alias("kwh_change_bucket"),
        F.abs(F.col("read_at_rank") - 4).alias("read_at_rank_distance"),
    )
    .orderBy(F.col("read_at_rank_distance").asc_nulls_last())
)
