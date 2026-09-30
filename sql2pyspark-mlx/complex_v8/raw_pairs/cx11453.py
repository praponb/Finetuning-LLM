from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_quality_by_rea = Window.partitionBy(F.date_trunc("month", "read_at"), "quality").orderBy("read_at_month", "id")
w_all_by_read_at_month = Window.orderBy("read_at_month", "id")
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").desc())
w_read_at_month = Window.partitionBy("read_at_month")
w_read_at_month_quality_by_rea_2 = Window.partitionBy(F.date_trunc("month", "read_at"), "quality").orderBy("read_at_month")
w_site_id_site_zone_by_read_at = Window.partitionBy("site_id", "site_zone").orderBy("read_at_month", "id")
w_all_by_read_at_month_running = Window.orderBy("read_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_read_at_month_per = Window.partitionBy("site_id").orderBy("read_at_month_percent_rank", "id")
w_quality_read_at_month_by_rea = Window.partitionBy("quality", "read_at_month").orderBy(F.col("read_at_month_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_voltage_percent_r = Window.partitionBy("quality").orderBy(F.col("voltage_percent_rank").desc(), "id").rowsBetween(-3, -1)
w_read_at_month_quality = Window.partitionBy("read_at_month", "quality")
w_site_id_by_last_read_at_full = Window.partitionBy("site_id").orderBy("last_read_at", "read_at_month", "site_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_last_read_at = Window.partitionBy("site_id").orderBy("last_read_at", "read_at_month", "site_id")
w_site_id_by_last_read_at_prio = Window.partitionBy("site_id").orderBy("last_read_at", "read_at_month", "site_id").rowsBetween(-3, -1)

recent = (
    df_meter_readings
    .filter((F.col("quality").isin("OK", "BAD")) & (F.col("read_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.month(F.col("read_at")).alias("read_at_month"),
    )
)

with_parent_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_month",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_windows = (
    with_parent_site
    .select(
        "*",
        F.round(F.percent_rank().over(w_read_at_month_quality_by_rea), 3).alias("read_at_month_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_read_at_month), 3).alias("read_at_month_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_voltage_ties), 3).alias("voltage_percent_rank"),
        F.count(F.lit(1)).over(w_read_at_month).alias("rows_in_read_at_month"),
        F.max("voltage").over(w_read_at_month).alias("read_at_month_max_voltage"),
        F.round(F.percent_rank().over(w_read_at_month_quality_by_rea_2), 3).alias("read_at_month_percent_rank_2"),
        F.lead("read_at_month").over(w_site_id_site_zone_by_read_at).alias("next_read_at_month"),
        F.max("kwh").over(w_all_by_read_at_month_running).alias("max_kwh_so_far"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "read_at_month",
        "site_installed_kw",
        "read_at_month_percent_rank",
        "read_at_month_cume_dist",
        "voltage_percent_rank",
        "read_at_month_max_voltage",
        "read_at_month_percent_rank_2",
        (F.col("voltage") + F.col("max_kwh_so_far")).alias("voltage_plus_max_kwh_so_far"),
    )
)

windowed = (
    scored
    .select(
        "*",
        (F.col("read_at_month_percent_rank_2") - F.lag("read_at_month_percent_rank_2").over(w_site_id_by_read_at_month_per)).alias("read_at_month_percent_rank_2_change"),
        F.nth_value("read_at_month_percent_rank_2", 2).over(w_quality_read_at_month_by_rea).alias("second_read_at_month_percent_rank_2"),
        F.max("read_at_month_max_voltage").over(w_quality_by_voltage_percent_r).alias("read_at_month_max_voltage_max_3_prior"),
        F.size(F.collect_set("site_id").over(w_read_at_month_quality)).alias("distinct_site_id_in_read_at_month_quality"),
    )
)

grouped = (
    windowed
    .filter(F.col("quality") == "BAD")
    .groupBy("read_at_month", "site_id")
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.sum("read_at_month_max_voltage_max_3_prior").alias("total_read_at_month_max_voltage_max_3_prior"),
        F.sum(F.when(F.col("quality").isin("OK", "EST"), 1).otherwise(0)).alias("n_quality_in_ok_est"),
        F.count(F.when(F.col("quality") != "OK", 1)).alias("n_quality_ok"),
        F.max("voltage_plus_max_kwh_so_far").alias("max_voltage_plus_max_kwh_so_far_check"),
    )
    .filter(F.col("max_voltage_plus_max_kwh_so_far_check") > 433.5)
    .drop("max_voltage_plus_max_kwh_so_far_check")
)

ranked = (
    grouped
    .select(
        "*",
        F.nth_value("last_read_at", 2).over(w_site_id_by_last_read_at_full).alias("second_last_read_at"),
        F.ntile(10).over(w_site_id_by_last_read_at).alias("last_read_at_decile"),
        F.lead("last_read_at").over(w_site_id_by_last_read_at).alias("next_last_read_at"),
        F.round(F.avg("n_quality_in_ok_est").over(w_site_id_by_last_read_at_prio), 2).alias("n_quality_in_ok_est_avg_3_prior"),
    )
)

result = (
    ranked
    .filter(F.col("last_read_at_decile") > 2)
    .select(
        "read_at_month",
        "site_id",
        "last_read_at",
        "total_read_at_month_max_voltage_max_3_prior",
        "second_last_read_at",
        F.round((F.unix_timestamp(F.col("next_last_read_at")) - F.unix_timestamp(F.col("second_last_read_at"))) / 60, 1).alias("minutes_second_last_read_at_to_next_last_read_at"),
    )
    .orderBy(F.col("site_id").desc())
)
