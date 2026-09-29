from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_moving_3 = Window.orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_commissioned_on_month_b = Window.partitionBy(F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("voltage").desc())
w_site_tariff_site_commissione = Window.partitionBy("site_tariff", F.date_trunc("month", "site_commissioned_on"))
w_all_by_read_at_2 = Window.orderBy(F.col("read_at").asc_nulls_last(), "id")
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").asc_nulls_last())
w_site_commissioned_on_month_b_2 = Window.partitionBy(F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("voltage").desc(), "id").rowsBetween(-3, Window.currentRow)
w_site_commissioned_on_month = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"))
w_all_by_voltage_full = Window.orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_commissioned_on_month_b_3 = Window.partitionBy(F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_commissioned_on_month_b_4 = Window.partitionBy(F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("voltage").desc(), "id")
w_site_commissioned_on_month_b_5 = Window.partitionBy(F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("voltage").desc(), "id").rowsBetween(-6, -1)

base = (
    df_meter_readings
    .filter(F.col("kwh").between(24.5, 113.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

enriched_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality_ref",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

metrics = (
    enriched_site
    .select(
        "*",
        F.round(F.avg("voltage").over(w_all_by_voltage_moving_3), 2).alias("voltage_avg_4"),
        F.dense_rank().over(w_all_by_read_at).alias("read_at_dense_rank"),
        F.rank().over(w_site_commissioned_on_month_b).alias("voltage_rank"),
        F.size(F.collect_set("site_id").over(w_site_tariff_site_commissione)).alias("distinct_site_id_in_site_tariff_site_commissione"),
        F.round(F.cume_dist().over(w_all_by_read_at_2), 3).alias("read_at_cume_dist"),
        F.rank().over(w_all_by_read_at_ties).alias("read_at_rank"),
        F.round(F.avg("voltage").over(w_site_commissioned_on_month_b_2), 2).alias("voltage_avg_4_2"),
        F.min("voltage").over(w_site_commissioned_on_month).alias("site_commissioned_on_month_min_voltage"),
        F.nth_value("voltage", 2).over(w_all_by_voltage_full).alias("second_voltage"),
        F.nth_value("site_commissioned_on", 2).over(w_site_commissioned_on_month_b_3).alias("second_site_commissioned_on"),
        F.round(F.cume_dist().over(w_site_commissioned_on_month_b_4), 3).alias("voltage_cume_dist"),
        F.round(F.avg("voltage").over(w_site_commissioned_on_month_b_5), 2).alias("voltage_avg_6_prior"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_commissioned_on_month), F.lit(0)), 4).alias("voltage_share_of_site_commissioned_on_month"),
        F.lag("site_commissioned_on", 2).over(w_all_by_read_at).alias("prev2_site_commissioned_on"),
        F.sum("voltage").over(w_site_tariff_site_commissione).alias("site_tariff_site_commissioned_on_month_sum_volta"),
    )
)

top_per_group = metrics.filter(F.col("read_at_rank") == 1)

summary = (
    top_per_group
    .groupBy(
        "site_id",
        F.date_trunc("month", "site_commissioned_on").alias("site_commissioned_on_month"),
        "distinct_site_id_in_site_tariff_site_commissione",
    )
    .agg(
        F.sum("voltage_avg_4_2").alias("total_voltage_avg_4_2"),
        F.round(F.avg("second_voltage"), 2).alias("avg_second_voltage"),
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("site_tariff") != "EV", 1)) / F.count("*"), 1).alias("pct_site_tariff_ev"),
        F.sum(F.when(F.col("site_tariff") == "TOU", F.col("read_at_rank")).otherwise(0)).alias("read_at_rank_site_tariff_tou"),
        F.max("voltage_avg_4_2").alias("max_voltage_avg_4_2_check"),
    )
    .filter(F.col("max_voltage_avg_4_2_check") > 220.5)
    .drop("max_voltage_avg_4_2_check")
)

result = (
    summary
    .select(
        "site_id",
        "distinct_site_id_in_site_tariff_site_commissione",
        "total_voltage_avg_4_2",
        "pct_site_tariff_ev",
        "read_at_rank_site_tariff_tou",
        (F.col("total_voltage_avg_4_2") - F.col("cnt")).alias("total_voltage_avg_4_2_minus_cnt"),
    )
    .orderBy("pct_site_tariff_ev", F.col("total_voltage_avg_4_2").asc())
)
