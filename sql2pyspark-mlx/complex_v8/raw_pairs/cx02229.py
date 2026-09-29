from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_rows_1d = Window.orderBy("prior_rows_1d", "id")
w_prior_rows_1d_read_at_year = Window.partitionBy("prior_rows_1d", F.year("read_at"))
w_prior_rows_1d_prior_rows_1d = Window.partitionBy("prior_rows_1d", "prior_rows_1d_tercile")
w_prior_rows_1d_by_prior_rows = Window.partitionBy("prior_rows_1d").orderBy(F.col("prior_rows_1d").desc(), "id").rowsBetween(-1, Window.currentRow)

in_window = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.voltage").alias("prior_voltage_1d"),
    )
)

with_windows = (
    in_window
    .select(
        "*",
        F.ntile(3).over(w_all_by_prior_rows_1d).alias("prior_rows_1d_tercile"),
        F.round(F.col("prior_rows_1d") / F.nullif(F.sum("prior_rows_1d").over(w_prior_rows_1d_read_at_year), F.lit(0)), 4).alias("prior_rows_1d_share_of_prior_rows_1d_read_at_yea"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_prior_rows_1d))) / 60, 1).alias("minutes_since_prev_read_at"),
    )
)

site_id_stats = (
    with_windows
    .groupBy("site_id")
    .agg(F.percentile_approx("prior_voltage_1d", 0.5).alias("median_prior_voltage_1d"))
)

vs_group = (
    with_windows.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.prior_voltage_1d") >= F.col("g.median_prior_voltage_1d"))
    .select(
        "s.*",
        "g.median_prior_voltage_1d",
        F.round(F.col("s.prior_voltage_1d") - F.col("g.median_prior_voltage_1d"), 2).alias("prior_voltage_1d_minus_median"),
    )
)

sequenced = (
    vs_group
    .select(
        "*",
        F.count(F.lit(1)).over(w_prior_rows_1d_prior_rows_1d).alias("rows_in_prior_rows_1d_prior_rows_1d_tercile"),
        F.round(F.col("median_prior_voltage_1d") / F.nullif(F.sum("median_prior_voltage_1d").over(w_prior_rows_1d_prior_rows_1d), F.lit(0)), 4).alias("median_prior_voltage_1d_share_of_prior_rows_1d_p"),
        F.round(F.avg("median_prior_voltage_1d").over(w_prior_rows_1d_by_prior_rows), 2).alias("median_prior_voltage_1d_avg_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_rows_1d",
        "prior_voltage_1d",
        "prior_rows_1d_tercile",
        "prior_rows_1d_share_of_prior_rows_1d_read_at_yea",
        "minutes_since_prev_read_at",
        "median_prior_voltage_1d",
        "rows_in_prior_rows_1d_prior_rows_1d_tercile",
        "median_prior_voltage_1d_share_of_prior_rows_1d_p",
        "median_prior_voltage_1d_avg_2",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
    .orderBy(F.col("prior_voltage_1d").desc(), "id")
)
