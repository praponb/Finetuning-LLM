from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows_by_n_rows_full = Window.partitionBy("n_rows").orderBy(F.col("n_rows").desc(), "read_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_by_n_rows = Window.partitionBy("n_rows").orderBy(F.col("n_rows").desc(), "read_at_month")
w_first_read_at_month_n_rows = Window.partitionBy(F.date_trunc("month", "first_read_at"), "n_rows")
w_n_rows_by_n_rows_2 = Window.partitionBy("n_rows").orderBy(F.col("n_rows").desc_nulls_first(), "read_at_month")
w_n_rows_by_n_rows_prior_3 = Window.partitionBy("n_rows").orderBy(F.col("n_rows").desc_nulls_first(), "read_at_month").rowsBetween(-3, -1)

covered = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.voltage").alias("prior_voltage_12h"),
    )
)

grouped = (
    covered
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.min("prior_voltage_12h").alias("min_prior_voltage_12h"),
        F.count("*").alias("n_rows"),
        F.min("read_at").alias("first_read_at"),
        F.sum("prior_rows_12h").alias("total_prior_rows_12h"),
        F.round(F.stddev("prior_voltage_12h"), 2).alias("stddev_prior_voltage_12h"),
    )
)

metrics = (
    grouped
    .select(
        "read_at_month",
        "min_prior_voltage_12h",
        "n_rows",
        "first_read_at",
        "total_prior_rows_12h",
        "stddev_prior_voltage_12h",
        F.nth_value("read_at_month", 2).over(w_n_rows_by_n_rows_full).alias("second_read_at_month"),
        F.ntile(10).over(w_n_rows_by_n_rows).alias("n_rows_decile"),
        F.count(F.lit(1)).over(w_first_read_at_month_n_rows).alias("rows_in_first_read_at_month_n_rows"),
        F.round(F.avg("n_rows").over(w_first_read_at_month_n_rows), 2).alias("first_read_at_month_n_rows_avg_n_rows"),
        F.round((F.unix_timestamp("read_at_month") - F.unix_timestamp(F.lag("read_at_month").over(w_n_rows_by_n_rows_2))) / 60, 1).alias("minutes_since_prev_read_at_month"),
        F.max("stddev_prior_voltage_12h").over(w_n_rows_by_n_rows_prior_3).alias("stddev_prior_voltage_12h_max_3_prior"),
    )
)

result = (
    metrics
    .select(
        "read_at_month",
        "min_prior_voltage_12h",
        "n_rows",
        "first_read_at",
        "stddev_prior_voltage_12h",
        "second_read_at_month",
        "n_rows_decile",
        "rows_in_first_read_at_month_n_rows",
        "first_read_at_month_n_rows_avg_n_rows",
        "stddev_prior_voltage_12h_max_3_prior",
    )
    .orderBy(
        "second_read_at_month",
        "first_read_at_month_n_rows_avg_n_rows",
        "read_at_month",
    )
    .limit(20)
)
