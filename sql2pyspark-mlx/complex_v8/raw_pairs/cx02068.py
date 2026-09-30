from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_tariff_by_volta = Window.partitionBy("site_id", "site_tariff").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_ties = Window.orderBy("kwh")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_site_tariff_site_id = Window.partitionBy("site_tariff", "site_id")
w_site_id_site_tariff_by_volta_2 = Window.partitionBy("site_id", "site_tariff").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_moving_6 = Window.orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_site_tariff_kwh_rank = Window.partitionBy("site_tariff", "kwh_rank")
w_site_tariff_kwh_rank_by_avg = Window.partitionBy("site_tariff", "kwh_rank").orderBy("avg_kwh_rank", "kwh_rank", "site_tariff")

base = (
    df_meter_readings
    .filter((F.col("voltage").between(215.6, 234.5)) & (F.col("read_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

with_parent = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_trends = (
    with_parent
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "site_tariff",
        F.nth_value("kwh", 2).over(w_site_id_site_tariff_by_volta).alias("second_kwh"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank"),
        F.lag("kwh", 2, 0).over(w_all_by_kwh).alias("prev2_kwh"),
        F.row_number().over(w_all_by_kwh).alias("seq_no"),
        F.max("kwh").over(w_site_tariff_site_id).alias("site_tariff_site_id_max_kwh"),
        F.lag("kwh", 1, 0).over(w_all_by_kwh).alias("prev_kwh"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.max("voltage").over(w_site_tariff_site_id).alias("site_tariff_site_id_max_voltage"),
        F.round(F.cume_dist().over(w_all_by_kwh), 3).alias("kwh_cume_dist"),
        F.last("voltage", ignorenulls=True).over(w_site_id_site_tariff_by_volta_2).alias("last_known_voltage"),
        F.round(F.avg("voltage").over(w_all_by_kwh_moving_6), 2).alias("voltage_avg_7"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_tariff_site_id), 2).alias("kwh_vs_site_tariff_site_id_avg"),
    )
)

rollup_base = (
    with_trends
    .groupBy("kwh_rank", "site_tariff")
    .agg(
        F.percentile_approx("kwh_vs_site_tariff_site_id_avg", 0.5).alias("median_kwh_vs_site_tariff_site_id_avg"),
        F.sum(F.when(F.col("site_tariff").isin("FLAT", "EV"), F.col("second_kwh")).otherwise(0)).alias("second_kwh_site_tariff_in_flat_ev"),
        F.array_sort(F.collect_set("site_tariff")).alias("site_tariff_list"),
        F.round(100 * F.count(F.when(F.col("site_tariff") == "EV", 1)) / F.count("*"), 1).alias("pct_site_tariff_ev"),
        F.round(F.avg("kwh_rank"), 2).alias("avg_kwh_rank"),
    )
)

metrics = (
    rollup_base
    .withColumn("rows_in_site_tariff_kwh_rank", F.count(F.lit(1)).over(w_site_tariff_kwh_rank))
    .withColumn("avg_kwh_rank_decile", F.ntile(10).over(w_site_tariff_kwh_rank_by_avg))
)

result = (
    metrics
    .select(
        "kwh_rank",
        "site_tariff",
        "median_kwh_vs_site_tariff_site_id_avg",
        "second_kwh_site_tariff_in_flat_ev",
        "site_tariff_list",
        "avg_kwh_rank",
        "rows_in_site_tariff_kwh_rank",
        "avg_kwh_rank_decile",
        F.abs(F.col("kwh_rank") - 5).alias("kwh_rank_distance"),
        F.size(F.col("site_tariff_list")).alias("site_tariff_list_count"),
    )
    .orderBy(F.col("avg_kwh_rank").desc())
)
