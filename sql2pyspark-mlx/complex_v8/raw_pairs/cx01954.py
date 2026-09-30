from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_site_id_by_rea = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at = Window.orderBy("read_at", "id")
w_all_by_read_at_2 = Window.orderBy(F.col("read_at").desc(), "id")
w_quality = Window.partitionBy("quality")
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_moving_1 = Window.orderBy("read_at", "id").rowsBetween(-1, Window.currentRow)
w_site_zone_site_id_by_kwh_ful = Window.partitionBy("site_zone", "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_all_by_read_at_3 = Window.orderBy(F.col("read_at").desc_nulls_first(), "id")
w_second_read_at_month_quality = Window.partitionBy(F.date_trunc("month", "second_read_at"), "quality")
w_site_id_by_last_known_read_a = Window.partitionBy("site_id").orderBy(F.col("last_known_read_at").desc(), "id")
w_all_by_kwh_change = Window.orderBy("kwh_change", "id")

recent = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("read_at") >= F.lit("2025-02-06 13:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

joined_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

with_trends = (
    joined_site
    .select(
        "*",
        F.count(F.lit(1)).over(w_all_by_read_at_running).alias("running_count"),
        F.first("kwh", ignorenulls=True).over(w_read_at_month_site_id_by_rea).alias("first_known_kwh"),
        F.ntile(3).over(w_all_by_read_at).alias("read_at_tercile"),
        F.lag("kwh").over(w_all_by_read_at).alias("prev_kwh"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_read_at_2)).alias("kwh_change"),
        F.min("kwh").over(w_quality).alias("quality_min_kwh"),
        F.nth_value("read_at", 2).over(w_all_by_read_at_full).alias("second_read_at"),
        F.round(F.avg("kwh").over(w_all_by_read_at_moving_1), 2).alias("kwh_avg_2"),
        F.last("read_at", ignorenulls=True).over(w_read_at_month_site_id_by_rea).alias("last_known_read_at"),
        F.first("kwh").over(w_site_zone_site_id_by_kwh_ful).alias("first_kwh"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_zone), F.lit(0)), 4).alias("kwh_share_of_quality_site_zone"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_read_at_3))) / 60, 1).alias("minutes_since_prev_read_at"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "read_at",
        "kwh",
        "site_zone",
        "running_count",
        "first_known_kwh",
        "read_at_tercile",
        "prev_kwh",
        "kwh_change",
        "quality_min_kwh",
        "kwh_avg_2",
        F.size(F.collect_set("site_id").over(w_second_read_at_month_quality)).alias("distinct_site_id_in_second_read_at_month_quality"),
        F.sum("first_known_kwh").over(w_second_read_at_month_quality).alias("second_read_at_month_quality_sum_first_known_kwh"),
        F.sum("kwh").over(w_second_read_at_month_quality).alias("second_read_at_month_quality_sum_kwh"),
        F.max("kwh_change").over(w_second_read_at_month_quality).alias("second_read_at_month_quality_max_kwh_change"),
        F.lag("first_known_kwh", 2).over(w_site_id_by_last_known_read_a).alias("prev2_first_known_kwh"),
        F.round(F.col("first_known_kwh") - F.avg("first_known_kwh").over(w_second_read_at_month_quality), 2).alias("first_known_kwh_vs_second_read_at_month_quality_"),
        F.lag("first_kwh").over(w_all_by_kwh_change).alias("prev_first_kwh"),
    )
)

result = (
    with_windows
    .filter(F.col("running_count") >= 3)
    .select(
        "id",
        "read_at",
        "site_zone",
        "running_count",
        "first_known_kwh",
        "read_at_tercile",
        "prev_kwh",
        "quality_min_kwh",
        "kwh_avg_2",
        "distinct_site_id_in_second_read_at_month_quality",
        "second_read_at_month_quality_sum_kwh",
        "second_read_at_month_quality_max_kwh_change",
        "prev2_first_known_kwh",
        "first_known_kwh_vs_second_read_at_month_quality_",
        "prev_first_kwh",
        F.round(F.col("prev_first_kwh") / F.nullif(F.col("first_known_kwh"), F.lit(0)), 3).alias("prev_first_kwh_per_first_known_kwh"),
    )
    .orderBy("second_read_at_month_quality_max_kwh_change")
)
