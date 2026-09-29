from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_voltage = Window.partitionBy("site_id", "quality").orderBy("voltage", "id")
w_all_by_voltage_moving_2 = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_voltage = Window.orderBy(F.col("voltage").asc_nulls_last(), "id")
w_quality_site_tariff = Window.partitionBy("quality", "site_tariff")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_site_tariff_by_volta = Window.partitionBy("site_id", "site_tariff").orderBy(F.col("voltage").desc(), "id")
w_site_tariff_by_voltage = Window.partitionBy("site_tariff").orderBy(F.col("voltage").desc_nulls_first(), "id")
w_site_id_site_tariff_by_volta_2 = Window.partitionBy("site_id", "site_tariff").orderBy(F.col("voltage").desc())
w_site_id_quality_by_voltage_r = Window.partitionBy("site_id", "quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_by_voltage_full = Window.partitionBy("site_tariff").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_quality = Window.partitionBy("quality")
w_all_by_max_voltage_so_far_fu = Window.orderBy("max_voltage_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_tariff_by_minutes_since = Window.partitionBy("site_tariff").orderBy(F.col("minutes_since_prev_read_at").asc_nulls_last(), "id")

scoped = (
    df_meter_readings
    .filter((F.col("site_id") >= 5) | (F.col("read_at") >= F.lit("2025-01-19 12:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.quality_slug",
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_windows = (
    with_parent
    .select(
        "id",
        "site_id",
        "quality",
        "site_tariff",
        F.lag("voltage", 1, 0).over(w_site_id_quality_by_voltage).alias("prev_voltage"),
        F.max("voltage").over(w_all_by_voltage_moving_2).alias("voltage_max_3"),
        F.rank().over(w_all_by_voltage).alias("voltage_rank"),
        F.lead("voltage").over(w_site_id_quality_by_voltage).alias("next_voltage"),
        F.min("voltage").over(w_quality_site_tariff).alias("quality_site_tariff_min_voltage"),
        F.sum("voltage").over(w_all_by_voltage_running).alias("running_voltage"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_id_site_tariff_by_volta))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.cume_dist().over(w_site_tariff_by_voltage), 3).alias("voltage_cume_dist"),
        F.ntile(4).over(w_site_tariff_by_voltage).alias("voltage_quartile"),
        F.round(F.cume_dist().over(w_site_id_site_tariff_by_volta_2), 3).alias("voltage_cume_dist_2"),
        F.last("voltage", ignorenulls=True).over(w_site_id_quality_by_voltage_r).alias("last_known_voltage"),
        F.max("voltage").over(w_all_by_voltage_running).alias("max_voltage_so_far"),
        F.nth_value("read_at", 2).over(w_site_tariff_by_voltage_full).alias("second_read_at"),
        F.lead("voltage", 3, 0).over(w_site_id_site_tariff_by_volta).alias("next3_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("voltage_rank") == 1)
    .drop("voltage_rank")
)

metrics = (
    top_per_group
    .select(
        "*",
        F.max("minutes_since_prev_read_at").over(w_quality).alias("quality_max_minutes_since_prev_read_at"),
        F.nth_value("running_voltage", 2).over(w_all_by_max_voltage_so_far_fu).alias("second_running_voltage"),
        F.lag("next3_voltage").over(w_site_tariff_by_minutes_since).alias("prev_next3_voltage"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "quality",
        "site_tariff",
        "voltage_max_3",
        "next_voltage",
        "running_voltage",
        "minutes_since_prev_read_at",
        "voltage_cume_dist",
        "voltage_quartile",
        "voltage_cume_dist_2",
        "max_voltage_so_far",
        "second_read_at",
        "voltage_vs_site_id_avg",
        "quality_max_minutes_since_prev_read_at",
        "second_running_voltage",
        "prev_next3_voltage",
    )
    .orderBy(F.col("voltage_quartile").desc())
)
