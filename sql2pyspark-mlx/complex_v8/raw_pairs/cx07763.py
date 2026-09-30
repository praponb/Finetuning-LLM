from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_by_days_since_read = Window.partitionBy("site_zone").orderBy("days_since_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality = Window.partitionBy("quality")
w_site_zone_read_at_to_date_by = Window.partitionBy("site_zone", F.to_date("read_at")).orderBy(F.col("read_at").desc())
w_site_id_read_at_month_by_kwh = Window.partitionBy("site_id", F.month("read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_site_id_read_at_month_by_kwh_2 = Window.partitionBy("site_id", F.month("read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id")
w_site_id_read_at_month_by_kwh_3 = Window.partitionBy("site_id", F.month("read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)

recent_readings = (
    df_meter_readings
    .filter((F.col("voltage") <= 233.1) & (F.col("quality") == "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_parent_site = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") == "Z3")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.days_since_read_at",
        F.col("si.zone").alias("site_zone"),
    )
)

with_trends = (
    with_parent_site
    .select(
        "*",
        F.nth_value("kwh", 2).over(w_site_zone_by_days_since_read).alias("second_kwh"),
        F.round(F.col("days_since_read_at") - F.avg("days_since_read_at").over(w_quality), 2).alias("days_since_read_at_vs_quality_avg"),
        F.size(F.collect_set("site_zone").over(w_quality)).alias("distinct_site_zone_in_quality"),
        F.round(F.cume_dist().over(w_site_zone_read_at_to_date_by), 3).alias("read_at_cume_dist"),
        F.max("days_since_read_at").over(w_site_id_read_at_month_by_kwh).alias("days_since_read_at_max_4"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.round(F.col("days_since_read_at") / F.nullif(F.sum("days_since_read_at").over(w_quality), F.lit(0)), 4).alias("days_since_read_at_share_of_quality"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.sum("kwh").over(w_quality).alias("quality_sum_kwh"),
        F.row_number().over(w_site_id_read_at_month_by_kwh_2).alias("rn"),
        F.rank().over(w_site_id_read_at_month_by_kwh_2).alias("kwh_rank"),
        F.round(F.avg("days_since_read_at").over(w_quality), 2).alias("quality_avg_days_since_read_at"),
        F.round(F.avg("kwh").over(w_quality), 2).alias("quality_avg_kwh"),
        F.round(F.avg("kwh").over(w_site_id_read_at_month_by_kwh_3), 2).alias("kwh_avg_2"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "kwh",
        "site_zone",
        "second_kwh",
        "days_since_read_at_vs_quality_avg",
        "distinct_site_zone_in_quality",
        "read_at_cume_dist",
        "days_since_read_at_max_4",
        "rows_in_quality",
        "days_since_read_at_share_of_quality",
        "kwh_vs_quality_avg",
        "quality_sum_kwh",
        "rn",
        "kwh_rank",
        "quality_avg_kwh",
        "kwh_avg_2",
        (F.floor(F.col("days_since_read_at") / 100) * 100).alias("days_since_read_at_bucket"),
    )
    .orderBy(
        F.col("kwh_avg_2").desc(),
        F.col("days_since_read_at_vs_quality_avg").asc_nulls_last(),
    )
)
