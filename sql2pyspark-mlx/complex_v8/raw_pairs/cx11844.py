from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_voltage_tier_by_kwh = Window.partitionBy("site_id", "voltage_tier").orderBy("kwh", "id")
w_site_tariff_voltage_tier_by = Window.partitionBy("site_tariff", "voltage_tier").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc())
w_site_tariff_voltage_tier_by_2 = Window.partitionBy("site_tariff", "voltage_tier").orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_site_tariff_voltage_tier_by_3 = Window.partitionBy("site_tariff", "voltage_tier").orderBy(F.col("read_at").desc())
w_site_tariff_voltage_tier_by_4 = Window.partitionBy("site_tariff", "voltage_tier").orderBy(F.col("read_at").desc(), "id")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_ties = Window.orderBy("kwh")
w_all_by_kwh_running = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_kwh_rank_by_read_at_percent = Window.partitionBy("kwh_rank").orderBy(F.col("read_at_percent_rank").desc())
w_site_id_next_read_at_date_mo = Window.partitionBy("site_id", F.date_trunc("month", "next_read_at_date"))
w_kwh_rank_by_read_at_percent_2 = Window.partitionBy("kwh_rank").orderBy(F.col("read_at_percent_rank").desc(), "id")
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 241.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 237.5, "A").when(F.col("voltage") >= 218.4, "B").otherwise("C").alias("voltage_tier"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

with_parent = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "read_at_date",
        "site_tariff",
        F.lead("read_at_date").over(w_site_id_voltage_tier_by_kwh).alias("next_read_at_date"),
        F.first("read_at_date", ignorenulls=True).over(w_site_tariff_voltage_tier_by).alias("first_known_read_at_date"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        F.sum("kwh").over(w_site_tariff_voltage_tier_by_2).alias("kwh_sum_3"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.percent_rank().over(w_all_by_kwh), 3).alias("kwh_percent_rank"),
        F.row_number().over(w_all_by_read_at).alias("read_at_seq"),
        F.round(F.percent_rank().over(w_site_tariff_voltage_tier_by_3), 3).alias("read_at_percent_rank"),
        (F.col("site_installed_kw") - F.lag("site_installed_kw").over(w_site_tariff_voltage_tier_by_4)).alias("site_installed_kw_change"),
        F.count(F.lit(1)).over(w_all_by_read_at_running).alias("running_count"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.last("read_at", ignorenulls=True).over(w_all_by_kwh_running).alias("last_known_read_at"),
        F.lead("kwh").over(w_all_by_kwh).alias("next_kwh"),
        (F.col("site_installed_kw") - F.lag("site_installed_kw").over(w_all_by_kwh)).alias("site_installed_kw_change_2"),
    )
)

top_rows = (
    windowed
    .filter((F.col("read_at_dense_rank") == 1) & (F.col("site_id").isNotNull()))
    .drop("read_at_dense_rank")
)

metrics = (
    top_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "read_at_date",
        "site_tariff",
        "kwh_sum_3",
        "kwh_percent_rank",
        "read_at_seq",
        "read_at_percent_rank",
        "site_installed_kw_change",
        "running_count",
        "kwh_rank",
        "last_known_read_at",
        "next_kwh",
        "site_installed_kw_change_2",
        F.round(F.percent_rank().over(w_kwh_rank_by_read_at_percent), 3).alias("read_at_percent_rank_percent_rank"),
        F.round(F.col("read_at_seq") / F.nullif(F.sum("read_at_seq").over(w_site_id_next_read_at_date_mo), F.lit(0)), 4).alias("read_at_seq_share_of_site_id_next_read_at_date_m"),
        F.row_number().over(w_kwh_rank_by_read_at_percent_2).alias("seq_no"),
        F.min("read_at_seq").over(w_site_id_by_kwh_running).alias("min_read_at_seq_so_far"),
        F.first("voltage").over(w_site_id_by_kwh_full).alias("first_voltage"),
    )
)

summary = (
    metrics
    .groupBy(
        "read_at_seq",
        F.when(F.col("first_voltage") >= 238.8, "large").when(F.col("first_voltage") >= 220.7, "medium").otherwise("small").alias("first_voltage_tier"),
        "site_id",
    )
    .agg(
        F.percentile_approx("kwh_percent_rank", 0.5).alias("median_kwh_percent_rank"),
        F.count(F.when(~F.col("site_tariff").isin("EV", "TOU"), 1)).alias("n_site_tariff_not_in_ev_tou"),
        F.round(F.avg("kwh_percent_rank"), 2).alias("avg_kwh_percent_rank"),
    )
)

result = (
    summary
    .select(
        "read_at_seq",
        "first_voltage_tier",
        "site_id",
        "median_kwh_percent_rank",
        "n_site_tariff_not_in_ev_tou",
        "avg_kwh_percent_rank",
    )
    .orderBy(
        F.col("avg_kwh_percent_rank").desc(),
        F.col("first_voltage_tier").desc_nulls_first(),
    )
)
