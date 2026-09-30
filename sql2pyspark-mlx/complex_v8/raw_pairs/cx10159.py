from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_commissioned_on_year_si = Window.partitionBy(F.year("site_commissioned_on"), "site_id").orderBy("read_at", "id").rowsBetween(-3, Window.currentRow)
w_site_commissioned_on_year_si_2 = Window.partitionBy(F.year("site_commissioned_on"), "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_commissioned_on_year_qu = Window.partitionBy(F.year("site_commissioned_on"), "quality").orderBy(F.col("read_at_dayofweek").desc())
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_quality_by_site_commissioned = Window.partitionBy("quality").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_site_commissioned_2 = Window.partitionBy("quality").orderBy(F.col("site_commissioned_on").desc(), "id")
w_site_commissioned_on_year_qu_2 = Window.partitionBy(F.year("site_commissioned_on"), "quality").orderBy(F.col("read_at_dayofweek").desc(), "id")
w_site_commissioned_on_year_si_3 = Window.partitionBy(F.year("site_commissioned_on"), "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_site_commissioned_3 = Window.partitionBy("quality").orderBy(F.col("site_commissioned_on").desc())
w_site_commissioned_on_dense_r = Window.partitionBy("site_commissioned_on_dense_rank", F.year("second_read_at"))
w_site_commissioned_on_month_b = Window.partitionBy(F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("first_site_installed_kw").desc())

recent = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

with_details_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

metrics = (
    with_details_site
    .select(
        "*",
        F.round(F.avg("read_at_dayofweek").over(w_site_commissioned_on_year_si), 2).alias("read_at_dayofweek_avg_4"),
        F.first("site_installed_kw", ignorenulls=True).over(w_site_commissioned_on_year_si_2).alias("first_known_site_installed_kw"),
        F.rank().over(w_site_commissioned_on_year_qu).alias("read_at_dayofweek_rank"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_quality_site_id), 2).alias("site_installed_kw_vs_quality_site_id_avg"),
        F.nth_value("read_at", 2).over(w_quality_by_site_commissioned).alias("second_read_at"),
        F.round(F.cume_dist().over(w_quality_by_site_commissioned_2), 3).alias("site_commissioned_on_cume_dist"),
        F.lead("read_at_dayofweek").over(w_site_commissioned_on_year_qu_2).alias("next_read_at_dayofweek"),
        F.nth_value("site_commissioned_on", 2).over(w_site_commissioned_on_year_si_3).alias("second_site_commissioned_on"),
        F.dense_rank().over(w_quality_by_site_commissioned_3).alias("site_commissioned_on_dense_rank"),
        F.round(F.percent_rank().over(w_site_commissioned_on_year_qu), 3).alias("read_at_dayofweek_percent_rank"),
        F.round(F.col("voltage_plus_kwh") - F.avg("voltage_plus_kwh").over(w_quality_site_id), 2).alias("voltage_plus_kwh_vs_quality_site_id_avg"),
        F.lag("voltage", 1, 0).over(w_quality_by_site_commissioned_2).alias("prev_voltage"),
        F.first("site_installed_kw").over(w_quality_by_site_commissioned).alias("first_site_installed_kw"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
        F.min("read_at_dayofweek").over(w_quality_site_id).alias("quality_site_id_min_read_at_dayofweek"),
    )
)

with_trends = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "read_at_dayofweek",
        "site_commissioned_on",
        "site_name",
        "site_installed_kw",
        "first_known_site_installed_kw",
        "site_installed_kw_vs_quality_site_id_avg",
        "second_read_at",
        "site_commissioned_on_dense_rank",
        "read_at_dayofweek_percent_rank",
        "voltage_plus_kwh_vs_quality_site_id_avg",
        "prev_voltage",
        "voltage_share_of_quality_site_id",
        F.count(F.lit(1)).over(w_site_commissioned_on_dense_r).alias("rows_in_site_commissioned_on_dense_rank_second_r"),
        F.round(F.col("prev_voltage") - F.avg("prev_voltage").over(w_site_commissioned_on_dense_r), 2).alias("prev_voltage_vs_site_commissioned_on_dense_rank_"),
        F.sum("next_read_at_dayofweek").over(w_site_commissioned_on_dense_r).alias("site_commissioned_on_dense_rank_second_read_at_y"),
        F.dense_rank().over(w_site_commissioned_on_month_b).alias("first_site_installed_kw_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "read_at_dayofweek",
        "site_commissioned_on",
        "site_name",
        "site_installed_kw",
        "first_known_site_installed_kw",
        "site_installed_kw_vs_quality_site_id_avg",
        "site_commissioned_on_dense_rank",
        "read_at_dayofweek_percent_rank",
        "voltage_plus_kwh_vs_quality_site_id_avg",
        "prev_voltage",
        "prev_voltage_vs_site_commissioned_on_dense_rank_",
        "site_commissioned_on_dense_rank_second_read_at_y",
        "first_site_installed_kw_dense_rank",
        F.greatest(F.col("prev_voltage"), F.col("site_installed_kw")).alias("greatest_prev_voltage_site_installed_kw"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)
