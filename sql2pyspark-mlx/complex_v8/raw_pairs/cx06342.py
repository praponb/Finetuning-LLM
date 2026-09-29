from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone = Window.partitionBy("site_zone")
w_all_by_voltage_distance = Window.orderBy("voltage_distance", "id")
w_site_tariff_site_zone_by_vol = Window.partitionBy("site_tariff", "site_zone").orderBy(F.col("voltage").desc())
w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage_distance_movi = Window.orderBy("voltage_distance", "id").rowsBetween(-2, Window.currentRow)
w_site_id_by_read_at_moving_3 = Window.partitionBy("site_id").orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_distinct_site_tariff_in_site = Window.partitionBy("distinct_site_tariff_in_site_zone")
w_site_zone_distinct_site_tari = Window.partitionBy("site_zone", "distinct_site_tariff_in_site_zone")
w_quality_site_id_by_last_know = Window.partitionBy("quality", "site_id").orderBy(F.col("last_known_voltage_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = (
    df_meter_readings
    .filter((F.col("kwh") > 48.2) | (F.col("quality").isin("EST", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 223.1).alias("voltage_distance"),
    )
)

combined_details = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "EV"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.voltage_distance",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

sequenced = (
    combined_details
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "voltage_distance",
        "site_name",
        "site_zone",
        F.size(F.collect_set("site_tariff").over(w_site_zone)).alias("distinct_site_tariff_in_site_zone"),
        F.lag("voltage").over(w_all_by_voltage_distance).alias("prev_voltage"),
        F.round(F.cume_dist().over(w_site_tariff_site_zone_by_vol), 3).alias("voltage_cume_dist"),
        F.max("voltage").over(w_site_zone).alias("site_zone_max_voltage"),
        F.count(F.lit(1)).over(w_site_id_site_tariff).alias("rows_in_site_id_site_tariff"),
        F.max("voltage_distance").over(w_site_zone).alias("site_zone_max_voltage_distance"),
        F.last("voltage_distance", ignorenulls=True).over(w_site_id_by_voltage_running).alias("last_known_voltage_distance"),
        F.max("voltage").over(w_all_by_voltage_distance_movi).alias("voltage_max_3"),
        F.round(F.avg("voltage_distance").over(w_site_id_site_tariff), 2).alias("site_id_site_tariff_avg_voltage_distance"),
        F.sum("voltage").over(w_site_id_by_read_at_moving_3).alias("voltage_sum_4"),
    )
)

annotated = (
    sequenced
    .filter(F.col("rows_in_site_id_site_tariff") <= 16)
    .select(
        "*",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

with_windows = (
    annotated
    .select(
        "id",
        "site_id",
        "voltage",
        "voltage_distance",
        "site_name",
        "site_zone",
        "distinct_site_tariff_in_site_zone",
        "prev_voltage",
        "voltage_cume_dist",
        "site_zone_max_voltage_distance",
        "last_known_voltage_distance",
        "voltage_max_3",
        "site_id_site_tariff_avg_voltage_distance",
        "voltage_sum_4",
        "read_at_label",
        F.round(F.col("site_zone_max_voltage") - F.avg("site_zone_max_voltage").over(w_distinct_site_tariff_in_site), 2).alias("site_zone_max_voltage_vs_distinct_site_tariff_in"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_zone_distinct_site_tari), F.lit(0)), 4).alias("voltage_share_of_site_zone_distinct_site_tariff_"),
        F.min("last_known_voltage_distance").over(w_quality_site_id_by_last_know).alias("min_last_known_voltage_distance_so_far"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_distinct_site_tariff_in_site), 2).alias("voltage_vs_distinct_site_tariff_in_site_zone_avg"),
    )
)

result = (
    with_windows
    .filter(F.col("read_at_label").isNotNull())
    .select("site_id_site_tariff_avg_voltage_distance")
    .distinct()
    .orderBy(F.col("site_id_site_tariff_avg_voltage_distance").desc())
)
