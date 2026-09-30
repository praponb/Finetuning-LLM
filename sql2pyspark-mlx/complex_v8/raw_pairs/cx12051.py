from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh_running = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id = Window.partitionBy("site_id")
w_quality = Window.partitionBy("quality")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_quality_by_kwh_full = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_readings = (
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

sequenced = (
    valid_readings
    .select(
        "*",
        F.last("kwh", ignorenulls=True).over(w_quality_by_kwh_running).alias("last_known_kwh"),
        F.sum("kwh").over(w_quality_site_id).alias("quality_site_id_sum_kwh"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_id_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_id), 2).alias("kwh_vs_quality_site_id_avg"),
        F.row_number().over(w_all_by_kwh).alias("rn"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.last("voltage").over(w_quality_by_kwh_full).alias("last_voltage"),
    )
)

result = (
    sequenced
    .filter(F.col("voltage") <= 241.8)
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "last_known_kwh",
        "quality_site_id_sum_kwh",
        "voltage_vs_site_id_avg",
        "voltage_vs_quality_avg",
        "minutes_since_prev_read_at",
        "kwh_vs_quality_site_id_avg",
        "rn",
        "kwh_share_of_quality",
        "rows_in_quality_site_id",
    )
    .orderBy(F.col("voltage_vs_quality_avg").asc())
)
