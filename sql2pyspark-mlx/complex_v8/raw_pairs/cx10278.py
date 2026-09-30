from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_read_at_hour_runn = Window.partitionBy("quality").orderBy(F.col("read_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_quality_by_read_at_hour_prio = Window.partitionBy("quality").orderBy(F.col("read_at_hour").desc(), "id").rowsBetween(-2, -1)
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_hour_month_site_id = Window.partitionBy(F.month("read_at_hour"), "site_id")
w_all_by_read_at_2 = Window.orderBy("read_at", "id")
w_read_at_quintile_by_pct_qual = Window.partitionBy("read_at_quintile").orderBy(F.col("pct_quality_not_in_ok_bad").desc(), "read_at_quintile", "read_at_month")
w_read_at_month_year_by_first = Window.partitionBy(F.year("read_at_month")).orderBy(F.col("first_prev_read_at").desc(), "read_at_quintile", "read_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_quintile_by_pct_qual_2 = Window.partitionBy("read_at_quintile").orderBy(F.col("pct_quality_not_in_ok_bad").desc(), "read_at_quintile", "read_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_meter_readings
    .filter(F.col("voltage") < 240.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.first("kwh", ignorenulls=True).over(w_quality_by_read_at_hour_runn).alias("first_known_kwh"),
        F.lag("read_at").over(w_all_by_read_at).alias("prev_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.round(F.avg("kwh").over(w_quality_by_read_at_hour_prio), 2).alias("kwh_avg_2_prior"),
        F.nth_value("read_at", 2).over(w_all_by_read_at_full).alias("second_read_at"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.sum("kwh").over(w_read_at_hour_month_site_id).alias("read_at_hour_month_site_id_sum_kwh"),
        F.ntile(5).over(w_all_by_read_at_2).alias("read_at_quintile"),
    )
)

grouped = (
    metrics
    .groupBy(
        "read_at_quintile",
        F.date_trunc("month", "read_at").alias("read_at_month"),
    )
    .agg(
        F.min("prev_read_at").alias("first_prev_read_at"),
        F.round(100 * F.count(F.when(~F.col("quality").isin("OK", "BAD"), 1)) / F.count("*"), 1).alias("pct_quality_not_in_ok_bad"),
        F.max("kwh_avg_2_prior").alias("max_kwh_avg_2_prior_check"),
    )
    .filter(F.col("max_kwh_avg_2_prior_check") > 12.0)
    .drop("max_kwh_avg_2_prior_check")
)

metrics_2 = (
    grouped
    .select(
        "*",
        F.lag("first_prev_read_at").over(w_read_at_quintile_by_pct_qual).alias("prev_first_prev_read_at"),
        F.last("read_at_quintile", ignorenulls=True).over(w_read_at_month_year_by_first).alias("last_known_read_at_quintile"),
        F.last("read_at_month").over(w_read_at_quintile_by_pct_qual_2).alias("last_read_at_month"),
        F.last("read_at_month", ignorenulls=True).over(w_read_at_month_year_by_first).alias("last_known_read_at_month"),
    )
)

result = (
    metrics_2
    .select(
        "read_at_quintile",
        "read_at_month",
        "pct_quality_not_in_ok_bad",
        "last_known_read_at_quintile",
        "last_known_read_at_month",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at_month")).alias("days_since_read_at_month"),
    )
    .orderBy(F.col("last_known_read_at_quintile").desc())
)
