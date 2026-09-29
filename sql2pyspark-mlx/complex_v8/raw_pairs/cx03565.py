from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_zone = Window.partitionBy("site_id", "site_zone")
w_site_zone = Window.partitionBy("site_zone")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_all_by_site_commissioned_on = Window.orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_by_min_kwh_vs_site = Window.partitionBy("site_zone").orderBy(F.col("min_kwh_vs_site_id_site_zone_avg").desc(), "read_at_month", "site_zone").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_by_min_kwh_vs_site_2 = Window.partitionBy("site_zone").orderBy(F.col("min_kwh_vs_site_id_site_zone_avg").desc(), "read_at_month", "site_zone")
w_site_zone_by_min_kwh_vs_site_3 = Window.partitionBy("site_zone").orderBy(F.col("min_kwh_vs_site_id_site_zone_avg").desc(), "read_at_month", "site_zone").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

joined = (
    base.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").isNotNull()), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_site_zone), 2).alias("kwh_vs_site_id_site_zone_avg"),
        F.sum("kwh").over(w_site_zone).alias("site_zone_sum_kwh"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_zone), F.lit(0)), 4).alias("kwh_share_of_site_zone"),
        F.row_number().over(w_all_by_kwh).alias("kwh_seq"),
        F.last("kwh").over(w_all_by_site_commissioned_on).alias("last_kwh"),
    )
)

first_rows = (
    with_trends
    .filter(F.col("kwh_seq") <= 3)
    .drop("kwh_seq")
)

stats = (
    first_rows
    .filter(F.col("site_commissioned_on") >= F.lit("2024-08-24").cast("date"))
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"), "site_zone")
    .agg(
        F.sum("kwh_vs_site_id_site_zone_avg").alias("total_kwh_vs_site_id_site_zone_avg"),
        F.round(F.sum("last_kwh") / F.nullif(F.sum("site_zone_sum_kwh"), F.lit(0)), 3).alias("last_kwh_to_site_zone_sum_kwh_ratio"),
        F.min("kwh_vs_site_id_site_zone_avg").alias("min_kwh_vs_site_id_site_zone_avg"),
        F.max("kwh").alias("max_kwh"),
        F.avg("kwh_vs_site_id_site_zone_avg").alias("avg_kwh_vs_site_id_site_zone_avg_check"),
    )
    .filter(F.col("avg_kwh_vs_site_id_site_zone_avg_check") > -96.0)
    .drop("avg_kwh_vs_site_id_site_zone_avg_check")
)

with_windows = (
    stats
    .select(
        "read_at_month",
        "site_zone",
        "total_kwh_vs_site_id_site_zone_avg",
        "last_kwh_to_site_zone_sum_kwh_ratio",
        F.first("max_kwh", ignorenulls=True).over(w_site_zone_by_min_kwh_vs_site).alias("first_known_max_kwh"),
        F.row_number().over(w_site_zone_by_min_kwh_vs_site_2).alias("rn"),
        F.nth_value("max_kwh", 2).over(w_site_zone_by_min_kwh_vs_site_3).alias("second_max_kwh"),
    )
)

result = (
    with_windows
    .select(
        "read_at_month",
        "site_zone",
        "first_known_max_kwh",
        "rn",
        "second_max_kwh",
        F.upper(F.trim(F.col("site_zone"))).alias("site_zone_clean"),
        F.date_trunc("year", F.col("read_at_month")).alias("read_at_month_year"),
    )
)
