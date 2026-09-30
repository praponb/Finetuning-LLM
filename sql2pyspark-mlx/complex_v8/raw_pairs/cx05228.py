from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_site_id = Window.partitionBy(F.year("read_at"), "site_id")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_all_by_voltage_ties = Window.orderBy("voltage")
w_all_by_kwh = Window.orderBy(F.col("kwh").asc_nulls_last(), "id")
w_all_by_voltage_prior_1 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(-1, -1)
w_all_by_kwh_prior_2 = Window.orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_all_by_voltage_2 = Window.orderBy(F.col("voltage").desc(), "id")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn = Window.partitionBy("rn")
w_site_id_rn_by_voltage_cume_d = Window.partitionBy("site_id", "rn").orderBy(F.col("voltage_cume_dist").desc(), "id")
w_all_by_rn_moving_6 = Window.orderBy("rn", "id").rowsBetween(-6, Window.currentRow)
w_site_id_rn_by_voltage_cume_d_2 = Window.partitionBy("site_id", "rn").orderBy(F.col("voltage_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rn = Window.orderBy("rn", "id")

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_windows = (
    filtered_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_year_site_id), 2).alias("kwh_vs_read_at_year_site_id_avg"),
        F.lag("voltage", 2).over(w_all_by_voltage).alias("prev2_voltage"),
        F.round(F.cume_dist().over(w_all_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.lag("voltage", 1, 0).over(w_all_by_kwh).alias("prev_voltage"),
        F.sum("voltage").over(w_all_by_voltage_prior_1).alias("voltage_sum_1_prior"),
        F.round(F.avg("voltage").over(w_all_by_kwh_prior_2), 2).alias("voltage_avg_2_prior"),
        F.count(F.lit(1)).over(w_read_at_year_site_id).alias("rows_in_read_at_year_site_id"),
        F.row_number().over(w_all_by_voltage_2).alias("rn"),
        F.first("kwh", ignorenulls=True).over(w_all_by_voltage_running).alias("first_known_kwh"),
        F.last("kwh").over(w_site_id_by_kwh_full).alias("last_kwh"),
    )
)

annotated = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh_vs_read_at_year_site_id_avg",
        "prev2_voltage",
        "voltage_cume_dist",
        "prev_voltage",
        "voltage_avg_2_prior",
        "rows_in_read_at_year_site_id",
        "rn",
        F.year(F.col("read_at")).alias("read_at_year"),
        F.coalesce(F.col("prev_voltage"), F.lit(0.0)).alias("prev_voltage_filled"),
        F.round(F.col("rn") / F.nullif(F.col("prev2_voltage"), F.lit(0)), 3).alias("rn_per_prev2_voltage"),
    )
)

windowed = (
    annotated
    .select(
        "*",
        F.round(F.col("prev2_voltage") / F.nullif(F.sum("prev2_voltage").over(w_rn), F.lit(0)), 4).alias("prev2_voltage_share_of_rn"),
        F.lag("prev_voltage_filled", 2, 0).over(w_site_id_rn_by_voltage_cume_d).alias("prev2_prev_voltage_filled"),
        F.count(F.lit(1)).over(w_rn).alias("rows_in_rn"),
        F.max("voltage_avg_2_prior").over(w_all_by_rn_moving_6).alias("voltage_avg_2_prior_max_7"),
        F.count(F.lit(1)).over(w_site_id_rn_by_voltage_cume_d_2).alias("running_count"),
        F.round(F.cume_dist().over(w_site_id_rn_by_voltage_cume_d), 3).alias("voltage_cume_dist_cume_dist"),
        (F.col("rn_per_prev2_voltage") - F.lag("rn_per_prev2_voltage").over(w_all_by_rn)).alias("rn_per_prev2_voltage_change"),
        F.ntile(10).over(w_all_by_rn).alias("rn_decile"),
    )
)

summary = (
    windowed
    .filter(F.col("kwh_vs_read_at_year_site_id_avg").between(-72.0, 84.0))
    .groupBy("read_at_year", "rn_decile", "site_id")
    .agg(F.min("read_at").alias("first_read_at"))
)

result = (
    summary
    .select(
        "read_at_year",
        "rn_decile",
        "site_id",
        "first_read_at",
        (F.col("rn_decile") - F.col("read_at_year")).alias("rn_decile_minus_read_at_year"),
    )
    .orderBy(F.col("rn_decile_minus_read_at_year").desc())
)
