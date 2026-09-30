from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_by_read_at = Window.partitionBy("site_zone").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_quality_site_id_by_kwh_dista = Window.partitionBy("quality", "site_id").orderBy("kwh_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_site_id = Window.partitionBy("site_tariff", "site_id")
w_all_by_kwh_distance_running = Window.orderBy(F.col("kwh_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_kwh_dista_2 = Window.partitionBy("quality", "site_id").orderBy("kwh_distance")
w_all_by_kwh_distance = Window.orderBy(F.col("kwh_distance").asc_nulls_last(), "id")
w_quality_site_id_by_kwh_dista_3 = Window.partitionBy("quality", "site_id").orderBy("kwh_distance", "id").rowsBetween(-2, -1)
w_site_tariff = Window.partitionBy("site_tariff")
w_all_by_kwh_distance_full = Window.orderBy(F.col("kwh_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_by_read_at_ties = Window.partitionBy("site_zone").orderBy(F.col("read_at").asc_nulls_last())
w_quality_by_next_voltage_ties = Window.partitionBy("quality").orderBy(F.col("next_voltage").desc())
w_all_by_voltage = Window.orderBy("voltage", "id")
w_all_by_voltage_full = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_zone_distinct_site_zone = Window.partitionBy("site_zone", "distinct_site_zone_in_site_tariff_site_id")
w_site_id_quality_by_first_kno = Window.partitionBy("site_id", "quality").orderBy(F.col("first_known_voltage").desc(), "id")

base_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 81.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 84.9).alias("kwh_distance"),
        F.abs(F.col("voltage") - 227.1).alias("voltage_distance"),
    )
)

enriched = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isin("TOU", "FLAT"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.kwh_distance",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.rank().over(w_site_zone_by_read_at).alias("read_at_rank"),
        F.last("read_at", ignorenulls=True).over(w_quality_site_id_by_kwh_dista).alias("last_known_read_at"),
        F.size(F.collect_set("site_zone").over(w_site_tariff_site_id)).alias("distinct_site_zone_in_site_tariff_site_id"),
        F.first("voltage", ignorenulls=True).over(w_all_by_kwh_distance_running).alias("first_known_voltage"),
        F.round(F.cume_dist().over(w_quality_site_id_by_kwh_dista_2), 3).alias("kwh_distance_cume_dist"),
        F.lead("voltage", 1, 0).over(w_all_by_kwh_distance).alias("next_voltage"),
        F.round(F.avg("kwh_distance").over(w_quality_site_id_by_kwh_dista_3), 2).alias("kwh_distance_avg_2_prior"),
        F.min("kwh_distance").over(w_site_tariff).alias("site_tariff_min_kwh_distance"),
        F.last("voltage").over(w_all_by_kwh_distance_full).alias("last_voltage"),
        F.round(F.percent_rank().over(w_site_zone_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.round(F.col("kwh_distance") - F.avg("kwh_distance").over(w_site_tariff), 2).alias("kwh_distance_vs_site_tariff_avg"),
    )
)

first_rows = (
    sequenced
    .filter((F.col("read_at_rank") == 1) & (F.col("kwh_distance_vs_site_tariff_avg") >= -24.0))
    .drop("read_at_rank")
)

metrics = (
    first_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "site_name",
        "last_known_read_at",
        "first_known_voltage",
        "last_voltage",
        "read_at_percent_rank",
        F.round(F.cume_dist().over(w_quality_by_next_voltage_ties), 3).alias("next_voltage_cume_dist"),
        F.row_number().over(w_all_by_voltage).alias("rn"),
        F.first("next_voltage").over(w_all_by_voltage_full).alias("first_next_voltage"),
        F.round(F.col("first_known_voltage") / F.nullif(F.sum("first_known_voltage").over(w_site_zone_distinct_site_zone), F.lit(0)), 4).alias("first_known_voltage_share_of_site_zone_distinct_"),
        F.lag("first_known_voltage").over(w_site_id_quality_by_first_kno).alias("prev_first_known_voltage"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "site_name",
        "first_known_voltage",
        "last_voltage",
        "read_at_percent_rank",
        "next_voltage_cume_dist",
        "rn",
        "first_next_voltage",
        "prev_first_known_voltage",
    )
    .orderBy("last_voltage", "prev_first_known_voltage")
)
