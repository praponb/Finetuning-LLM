from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_site_zones_by_total = Window.partitionBy("distinct_site_zones").orderBy(F.col("total_kwh").desc(), "site_zone", "site_id")
w_site_id = Window.partitionBy("site_id")

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

with_parent_site = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_group",
        "s.read_at_label",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

rollup_base = (
    with_parent_site
    .filter(F.col("voltage") >= 214.3)
    .groupBy("site_zone", "site_id")
    .agg(
        F.countDistinct("site_zone").alias("distinct_site_zones"),
        F.round(F.sum("kwh"), 2).alias("total_kwh"),
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
        F.round(100 * F.count(F.when(F.col("site_zone").isNotNull(), 1)) / F.count("*"), 1).alias("pct_site_zone_is_not_null"),
        F.sum("voltage").alias("sum_voltage_check"),
    )
    .filter(F.col("sum_voltage_check") > 216.8)
    .drop("sum_voltage_check")
)

windowed = (
    rollup_base
    .select(
        "site_zone",
        "site_id",
        "distinct_site_zones",
        "total_kwh",
        "stddev_voltage",
        "pct_site_zone_is_not_null",
        F.ntile(10).over(w_distinct_site_zones_by_total).alias("total_kwh_decile"),
        F.sum("distinct_site_zones").over(w_site_id).alias("site_id_sum_distinct_site_zones"),
    )
)

line_level = (
    windowed.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")), "left")
    .select(
        "s.site_zone",
        "s.site_id",
        "s.total_kwh",
        "s.stddev_voltage",
        "s.pct_site_zone_is_not_null",
        "s.total_kwh_decile",
        "s.site_id_sum_distinct_site_zones",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

result = (
    line_level
    .cube("reading_quality", "site_zone")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("reading_voltage"), 2).alias("total_reading_voltage"),
        F.countDistinct("reading_id").alias("distinct_reading_ids"),
    )
    .orderBy(
        F.col("reading_quality").asc_nulls_last(),
        F.col("site_zone").asc_nulls_last(),
    )
)
