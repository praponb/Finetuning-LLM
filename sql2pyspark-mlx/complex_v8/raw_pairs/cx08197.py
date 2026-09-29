from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_read_at_month_by_min_a = Window.partitionBy(F.month("first_read_at")).orderBy("min_avg_kwh_minus_voltage", "quality", "read_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_read_at = Window.orderBy(F.col("first_read_at").desc(), "quality", "read_at_month")
w_quality_by_first_min_avg_kwh = Window.partitionBy("quality").orderBy(F.col("first_min_avg_kwh_minus_voltage").desc(), "quality", "read_at_month")
w_quality = Window.partitionBy("quality")
w_all_by_first_min_avg_kwh_min = Window.orderBy("first_min_avg_kwh_minus_voltage", "quality", "read_at_month")
w_quality_by_first_min_avg_kwh_2 = Window.partitionBy("quality").orderBy(F.col("first_min_avg_kwh_minus_voltage").desc(), "quality", "read_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_readings = (
    df_meter_readings
    .filter((F.col("site_id") >= 5) & (F.col("read_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

labeled = (
    recent_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_ref",
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

quality_stats = (
    labeled
    .groupBy("quality")
    .agg(F.avg("kwh_minus_voltage").alias("avg_kwh_minus_voltage"))
)

above_average = (
    labeled.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh_minus_voltage") > F.col("g.avg_kwh_minus_voltage"))
    .select(
        "s.*",
        "g.avg_kwh_minus_voltage",
        F.round(F.col("s.kwh_minus_voltage") - F.col("g.avg_kwh_minus_voltage"), 2).alias("kwh_minus_voltage_minus_avg"),
    )
)

summary = (
    above_average
    .groupBy("quality", F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.min("avg_kwh_minus_voltage").alias("min_avg_kwh_minus_voltage"),
        F.min("read_at").alias("first_read_at"),
        F.avg("voltage").alias("avg_voltage_check"),
    )
    .filter(F.col("avg_voltage_check") > 215.6)
    .drop("avg_voltage_check")
)

ranked = (
    summary
    .select(
        "quality",
        "read_at_month",
        "min_avg_kwh_minus_voltage",
        F.first("min_avg_kwh_minus_voltage").over(w_first_read_at_month_by_min_a).alias("first_min_avg_kwh_minus_voltage"),
        F.round(F.cume_dist().over(w_all_by_first_read_at), 3).alias("first_read_at_cume_dist"),
    )
)

metrics = (
    ranked
    .select(
        "quality",
        "read_at_month",
        "first_min_avg_kwh_minus_voltage",
        F.round((F.unix_timestamp("read_at_month") - F.unix_timestamp(F.lag("read_at_month").over(w_quality_by_first_min_avg_kwh))) / 60, 1).alias("minutes_since_prev_read_at_month"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.round(F.col("min_avg_kwh_minus_voltage") - F.avg("min_avg_kwh_minus_voltage").over(w_quality), 2).alias("min_avg_kwh_minus_voltage_vs_quality_avg"),
        F.lag("first_read_at_cume_dist").over(w_all_by_first_min_avg_kwh_min).alias("prev_first_read_at_cume_dist"),
        F.first("read_at_month").over(w_quality_by_first_min_avg_kwh_2).alias("first_read_at_month"),
    )
)

result = (
    metrics
    .select(
        "quality",
        "first_min_avg_kwh_minus_voltage",
        "minutes_since_prev_read_at_month",
        "rows_in_quality",
        "min_avg_kwh_minus_voltage_vs_quality_avg",
        "first_read_at_month",
        (F.col("read_at_month") + F.expr("INTERVAL 1 DAYS")).alias("read_at_month_plus_1d"),
        F.concat(F.col("quality"), F.lit("#"), F.col("rows_in_quality").cast("string")).alias("quality_ref"),
    )
    .orderBy(F.col("quality").asc())
)
