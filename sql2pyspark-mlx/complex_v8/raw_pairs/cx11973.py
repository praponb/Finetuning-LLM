from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy("read_at", "id")
w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy("kwh", "id")
w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.row_number().over(w_site_id_quality_by_kwh).alias("kwh_seq"),
        F.count(F.lit(1)).over(w_quality_site_zone).alias("rows_in_quality_site_zone"),
        F.min("kwh").over(w_quality_site_zone).alias("quality_site_zone_min_kwh"),
        F.min("kwh").over(w_all_by_read_at_running).alias("min_kwh_so_far"),
        F.round(F.avg("kwh").over(w_quality_site_zone), 2).alias("quality_site_zone_avg_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_zone), 2).alias("kwh_vs_quality_site_zone_avg"),
        F.first("read_at").over(w_all_by_read_at_full).alias("first_read_at"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_zone), F.lit(0)), 4).alias("kwh_share_of_quality_site_zone"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_zone",
        "minutes_since_prev_read_at",
        "kwh_seq",
        "rows_in_quality_site_zone",
        "quality_site_zone_min_kwh",
        "min_kwh_so_far",
        "first_read_at",
        "kwh_share_of_quality_site_zone",
        (F.col("first_read_at") + F.expr("INTERVAL 30 MINUTES")).alias("first_read_at_plus_30m"),
    )
    .orderBy(F.col("rows_in_quality_site_zone").desc())
)
