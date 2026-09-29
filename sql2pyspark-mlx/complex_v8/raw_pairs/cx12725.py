from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_zone_by_site_co = Window.partitionBy("site_id", "site_zone").orderBy("site_commissioned_on", "id")
w_site_id = Window.partitionBy("site_id")
w_site_zone_by_read_at = Window.partitionBy("site_zone").orderBy("read_at", "id")
w_site_zone_read_at_to_date_by = Window.partitionBy("site_zone", F.to_date("read_at")).orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone_read_at_to_date_by_2 = Window.partitionBy("site_zone", F.to_date("read_at")).orderBy("read_at")
w_site_zone_read_at_to_date_by_3 = Window.partitionBy("site_zone", F.to_date("read_at")).orderBy("read_at", "id")

prepared_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-02-18 07:00:00").cast("timestamp")) & (F.col("quality").isNotNull()))
)

combined_details = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "site_zone",
        F.ntile(5).over(w_site_id_site_zone_by_site_co).alias("site_commissioned_on_quintile"),
        F.max("kwh").over(w_site_id).alias("site_id_max_kwh"),
        F.size(F.collect_set("site_zone").over(w_site_id)).alias("distinct_site_zone_in_site_id"),
        (F.col("voltage") - F.lag("voltage").over(w_site_zone_by_read_at)).alias("voltage_change"),
        F.sum("voltage").over(w_site_id).alias("site_id_sum_voltage"),
        F.first("read_at", ignorenulls=True).over(w_site_zone_read_at_to_date_by).alias("first_known_read_at"),
        F.dense_rank().over(w_site_zone_read_at_to_date_by_2).alias("read_at_dense_rank"),
        F.ntile(5).over(w_site_zone_read_at_to_date_by_3).alias("read_at_quintile"),
    )
)

per_group = (
    with_windows
    .groupBy("site_zone", "distinct_site_zone_in_site_id", "read_at_dense_rank")
    .agg(
        F.sum(F.when(F.col("site_zone").isin("Z4", "Z3"), F.col("distinct_site_zone_in_site_id")).otherwise(0)).alias("distinct_site_zone_in_site_id_site_zone_in_z4_z3"),
        F.round(F.stddev("distinct_site_zone_in_site_id"), 2).alias("stddev_distinct_site_zone_in_site_id"),
        F.count("*").alias("row_count"),
        F.countDistinct("site_zone").alias("distinct_site_zones"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
)

result = (
    per_group
    .filter(F.col("distinct_site_zone_in_site_id") >= 4)
    .select(
        "site_zone",
        "distinct_site_zone_in_site_id",
        "read_at_dense_rank",
        "distinct_site_zone_in_site_id_site_zone_in_z4_z3",
        "stddev_distinct_site_zone_in_site_id",
        "row_count",
        "distinct_site_zones",
        "distinct_site_ids",
        F.when(F.col("distinct_site_zones") >= 7, "A").when(F.col("distinct_site_zones") >= 4, "B").otherwise("C").alias("distinct_site_zones_tier"),
        F.initcap(F.trim(F.col("site_zone"))).alias("site_zone_clean"),
    )
)
