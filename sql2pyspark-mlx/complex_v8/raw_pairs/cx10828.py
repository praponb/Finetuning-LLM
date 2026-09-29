from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_site_instal = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("site_installed_kw").asc_nulls_last())
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_read_at_month_by_site_instal_2 = Window.partitionBy(F.date_trunc("month", "read_at")).orderBy(F.col("site_installed_kw").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_site_commissio = Window.partitionBy(F.date_trunc("month", "read_at"), F.year("site_commissioned_on"))
w_site_commissioned_on_month = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"))
w_site_id = Window.partitionBy("site_id")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_read_at_month_site_id_by_vol = Window.partitionBy(F.month("read_at"), "site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_ties = Window.orderBy("kwh")
w_site_id_read_at_year_by_kwh = Window.partitionBy("site_id", F.year("read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_year = Window.partitionBy("site_id", "read_at_year")
w_all_by_kwh_percent_rank_2_fu = Window.orderBy(F.col("kwh_percent_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_readings = (
    df_meter_readings
    .filter((F.col("voltage") > 222.5) & (~F.col("quality").isin("EST", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"),
    )
)

with_details_site = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

windowed = (
    with_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "voltage_adj",
        "site_commissioned_on",
        F.round(F.cume_dist().over(w_read_at_month_by_site_instal), 3).alias("site_installed_kw_cume_dist"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_by_kwh)).alias("voltage_change"),
        F.first("voltage_adj").over(w_read_at_month_by_site_instal_2).alias("first_voltage_adj"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month_site_commissio), 2).alias("kwh_vs_read_at_month_site_commissioned_on_year_a"),
        F.min("voltage_adj").over(w_site_commissioned_on_month).alias("site_commissioned_on_month_min_voltage_adj"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.percent_rank().over(w_all_by_kwh), 3).alias("kwh_percent_rank"),
        F.row_number().over(w_site_id_by_kwh).alias("rn"),
        F.sum("voltage").over(w_read_at_month_site_id_by_vol).alias("running_voltage"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank_2"),
        F.nth_value("site_installed_kw", 2).over(w_site_id_read_at_year_by_kwh).alias("second_site_installed_kw"),
    )
)

derived = windowed.withColumn("read_at_year", F.year(F.col("read_at")))

with_windows = (
    derived
    .select(
        "id",
        "kwh",
        "voltage",
        "voltage_adj",
        "site_commissioned_on",
        "site_installed_kw_cume_dist",
        "voltage_change",
        "kwh_vs_read_at_month_site_commissioned_on_year_a",
        "site_commissioned_on_month_min_voltage_adj",
        "rows_in_site_id",
        "kwh_percent_rank",
        "running_voltage",
        "kwh_percent_rank_2",
        "second_site_installed_kw",
        "read_at_year",
        F.round(F.col("kwh_percent_rank") / F.nullif(F.sum("kwh_percent_rank").over(w_site_id_read_at_year), F.lit(0)), 4).alias("kwh_percent_rank_share_of_site_id_read_at_year"),
        F.max("first_voltage_adj").over(w_site_id_read_at_year).alias("site_id_read_at_year_max_first_voltage_adj"),
        F.last("kwh").over(w_all_by_kwh_percent_rank_2_fu).alias("last_kwh"),
        F.sum("running_voltage").over(w_site_id_read_at_year).alias("site_id_read_at_year_sum_running_voltage"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "kwh",
        "voltage",
        "voltage_adj",
        "site_commissioned_on",
        "site_installed_kw_cume_dist",
        "voltage_change",
        "kwh_vs_read_at_month_site_commissioned_on_year_a",
        "site_commissioned_on_month_min_voltage_adj",
        "rows_in_site_id",
        "kwh_percent_rank",
        "running_voltage",
        "kwh_percent_rank_2",
        "second_site_installed_kw",
        "site_id_read_at_year_max_first_voltage_adj",
        "last_kwh",
        "site_id_read_at_year_sum_running_voltage",
    )
    .orderBy(F.col("kwh_vs_read_at_month_site_commissioned_on_year_a").asc())
)
