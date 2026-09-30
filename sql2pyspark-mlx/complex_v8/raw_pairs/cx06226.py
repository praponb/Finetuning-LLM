from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_site_zone_by_read_at_running = Window.partitionBy("site_zone").orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_zone = Window.partitionBy("quality", "site_zone")
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_quality_by_read_at = Window.partitionBy("site_zone", "quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_by_read_at_ties = Window.partitionBy("site_zone").orderBy(F.col("read_at").asc_nulls_last())
w_site_zone_quality_by_read_at_2 = Window.partitionBy("site_zone", "quality").orderBy("read_at", "id")
w_read_at_to_date_by_read_at_t = Window.partitionBy(F.to_date("read_at")).orderBy("read_at")

prepared = (
    df_meter_readings
    .filter((F.col("kwh") <= 106.0) & (F.col("voltage") <= 237.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

joined = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

sequenced = (
    joined
    .select(
        "id",
        "quality",
        "site_zone",
        "site_name",
        F.max("voltage").over(w_quality).alias("quality_max_voltage"),
        F.min("voltage").over(w_quality).alias("quality_min_voltage"),
        F.min("voltage").over(w_site_zone_by_read_at_running).alias("min_voltage_so_far"),
        F.sum("voltage").over(w_quality_site_zone).alias("quality_site_zone_sum_voltage"),
        F.size(F.collect_set("site_id").over(w_quality_site_zone)).alias("distinct_site_id_in_quality_site_zone"),
        F.first("read_at", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_read_at"),
        F.first("read_at", ignorenulls=True).over(w_site_zone_quality_by_read_at).alias("first_known_read_at_2"),
        F.rank().over(w_site_zone_by_read_at_ties).alias("read_at_rank"),
        F.min("voltage").over(w_all_by_read_at_running).alias("min_voltage_so_far_2"),
        F.round(F.avg("voltage").over(w_quality_site_zone), 2).alias("quality_site_zone_avg_voltage"),
        F.lag("voltage").over(w_site_zone_quality_by_read_at_2).alias("prev_voltage"),
        F.round(F.cume_dist().over(w_read_at_to_date_by_read_at_t), 3).alias("read_at_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "quality",
        "site_zone",
        "site_name",
        "quality_max_voltage",
        "quality_min_voltage",
        "quality_site_zone_sum_voltage",
        "distinct_site_id_in_quality_site_zone",
        "first_known_read_at_2",
        "read_at_rank",
        "min_voltage_so_far_2",
    )
    .orderBy(
        F.col("distinct_site_id_in_quality_site_zone").asc(),
        F.col("id").desc(),
    )
)
