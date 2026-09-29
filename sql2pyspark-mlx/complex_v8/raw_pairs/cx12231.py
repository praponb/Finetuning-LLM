from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_site_commissioned = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").asc_nulls_last(), "id")
w_all_by_site_commissioned_on = Window.orderBy("site_commissioned_on", "id")
w_site_id_by_site_commissioned_2 = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").asc_nulls_last())
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_site_id_by_site_commissioned_3 = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_minutes_since_prev_re = Window.orderBy(F.col("minutes_since_prev_read_at").desc())
w_rn_site_id = Window.partitionBy("rn", "site_id")
w_site_tariff_by_read_at_prior = Window.partitionBy("site_tariff").orderBy("read_at", "id").rowsBetween(-6, -1)
w_site_tariff_next_site_commis = Window.partitionBy("site_tariff", F.date_trunc("month", "next_site_commissioned_on")).orderBy("rn")
w_site_id = Window.partitionBy("site_id")
w_rn = Window.partitionBy("rn")
w_all_by_next_kwh_avg_6_prior = Window.orderBy(F.col("next_kwh_avg_6_prior_to_voltage_vs_rn_site_id_av").desc(), "site_commissioned_on_month", "rn").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn_by_min_next_kwh = Window.partitionBy("rn").orderBy("min_next_kwh", "site_commissioned_on_month", "rn")

filtered = (
    df_meter_readings
    .filter(F.col("voltage").between(220.2, 241.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_details_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

windowed = (
    with_details_site
    .select(
        "*",
        F.datediff(F.col("site_commissioned_on"), F.lag("site_commissioned_on").over(w_site_id_by_site_commissioned)).alias("days_since_prev_site_commissioned_on"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_site_commissioned_on))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.lead("kwh").over(w_all_by_site_commissioned_on).alias("next_kwh"),
        F.lead("site_commissioned_on").over(w_all_by_site_commissioned_on).alias("next_site_commissioned_on"),
        F.round(F.cume_dist().over(w_site_id_by_site_commissioned_2), 3).alias("site_commissioned_on_cume_dist"),
        F.row_number().over(w_site_id_by_site_commissioned).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_voltage), 3).alias("voltage_percent_rank"),
        F.count(F.lit(1)).over(w_site_id_site_tariff).alias("rows_in_site_id_site_tariff"),
        F.round(F.percent_rank().over(w_site_id_by_site_commissioned), 3).alias("site_commissioned_on_percent_rank"),
        F.round(F.avg("voltage").over(w_site_id_by_site_commissioned_3), 2).alias("voltage_avg_3"),
        F.round(F.percent_rank().over(w_site_id_by_site_commissioned_2), 3).alias("site_commissioned_on_percent_rank_2"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_site_tariff), F.lit(0)), 4).alias("voltage_share_of_site_id_site_tariff"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_site_tariff), 2).alias("voltage_vs_site_id_site_tariff_avg"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_site_tariff), 2).alias("kwh_vs_site_id_site_tariff_avg"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "site_commissioned_on",
        "site_tariff",
        "site_name",
        "days_since_prev_site_commissioned_on",
        "next_kwh",
        "site_commissioned_on_cume_dist",
        "rn",
        "rows_in_site_id_site_tariff",
        "site_commissioned_on_percent_rank",
        "site_commissioned_on_percent_rank_2",
        "voltage_vs_site_id_site_tariff_avg",
        "kwh_vs_site_id_site_tariff_avg",
        F.dense_rank().over(w_all_by_minutes_since_prev_re).alias("minutes_since_prev_read_at_dense_rank"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_rn_site_id), 2).alias("voltage_vs_rn_site_id_avg"),
        F.round(F.avg("next_kwh").over(w_site_tariff_by_read_at_prior), 2).alias("next_kwh_avg_6_prior"),
        F.dense_rank().over(w_site_tariff_next_site_commis).alias("rn_dense_rank"),
        F.round(F.col("voltage_percent_rank") - F.avg("voltage_percent_rank").over(w_site_id), 2).alias("voltage_percent_rank_vs_site_id_avg"),
        F.size(F.collect_set("site_tariff").over(w_rn_site_id)).alias("distinct_site_tariff_in_rn_site_id"),
    )
)

per_group = (
    metrics
    .groupBy(
        F.date_trunc("month", "site_commissioned_on").alias("site_commissioned_on_month"),
        "rn",
    )
    .agg(
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
        F.min("next_kwh").alias("min_next_kwh"),
        F.round(F.sum("next_kwh_avg_6_prior") / F.nullif(F.sum("voltage_vs_rn_site_id_avg"), F.lit(0)), 3).alias("next_kwh_avg_6_prior_to_voltage_vs_rn_site_id_av"),
    )
)

with_trends = (
    per_group
    .select(
        "*",
        F.count(F.lit(1)).over(w_rn).alias("rows_in_rn"),
        F.last("median_kwh").over(w_all_by_next_kwh_avg_6_prior).alias("last_median_kwh"),
        F.round(F.cume_dist().over(w_rn_by_min_next_kwh), 3).alias("min_next_kwh_cume_dist"),
    )
)

result = (
    with_trends
    .filter(F.col("median_kwh") >= 18.0)
    .select(
        "site_commissioned_on_month",
        "rn",
        "min_next_kwh",
        "next_kwh_avg_6_prior_to_voltage_vs_rn_site_id_av",
        "rows_in_rn",
        "min_next_kwh_cume_dist",
        (F.col("rn") + F.col("median_kwh")).alias("rn_plus_median_kwh"),
        F.least(F.col("min_next_kwh"), F.col("rows_in_rn")).alias("least_min_next_kwh_rows_in_rn"),
    )
    .orderBy("min_next_kwh_cume_dist")
)
