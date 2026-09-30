from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_kwh_tier = Window.partitionBy(F.year("read_at"), "kwh_tier")
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id")
w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_distinct_quality_in_read_at = Window.partitionBy("distinct_quality_in_read_at_year_kwh_tier", "kwh_tier")
w_all_by_distinct_quality_in_r = Window.orderBy(F.col("distinct_quality_in_read_at_year_kwh_tier").asc_nulls_last(), "id").rowsBetween(-1, -1)

filtered = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")) & (F.col("quality").isin("OK", "EST")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 104.8, "HIGH").when(F.col("kwh") >= 45.0, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.abs(F.col("kwh") - 52.4).alias("kwh_distance"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_read_at_year_kwh_tier).alias("rows_in_read_at_year_kwh_tier"),
        F.ntile(3).over(w_quality_by_voltage).alias("voltage_tercile"),
        F.max("kwh").over(w_read_at_year_kwh_tier).alias("read_at_year_kwh_tier_max_kwh"),
        F.lag("voltage").over(w_quality_by_read_at).alias("prev_voltage"),
        F.size(F.collect_set("quality").over(w_read_at_year_kwh_tier)).alias("distinct_quality_in_read_at_year_kwh_tier"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.round(F.col("voltage_tercile") - F.avg("voltage_tercile").over(w_distinct_quality_in_read_at), 2).alias("voltage_tercile_vs_distinct_quality_in_read_at_y"),
        F.round(F.avg("voltage_tercile").over(w_all_by_distinct_quality_in_r), 2).alias("voltage_tercile_avg_1_prior"),
    )
)

enriched = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

tagged = (
    enriched
    .select(
        "*",
        F.quarter(F.col("site_commissioned_on")).alias("site_commissioned_on_quarter"),
        F.concat(F.col("site_zone"), F.lit("#"), F.col("voltage_tercile").cast("string")).alias("site_zone_ref"),
    )
)

enriched_2 = (
    tagged
    .select(
        "id",
        "site_id",
        "quality",
        "kwh_tier",
        "kwh_distance",
        "rows_in_read_at_year_kwh_tier",
        "voltage_tercile",
        "prev_voltage",
        "distinct_quality_in_read_at_year_kwh_tier",
        "voltage_tercile_vs_distinct_quality_in_read_at_y",
        "voltage_tercile_avg_1_prior",
        "site_installed_kw",
        "site_zone",
        "site_commissioned_on_quarter",
        "site_zone_ref",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.substring(F.trim(F.col("kwh_tier")), 1, 3).alias("kwh_tier_prefix"),
        F.coalesce(F.col("kwh_distance"), F.lit(0.0)).alias("kwh_distance_filled"),
    )
)

line_level = (
    enriched_2.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.kwh_tier",
        "s.rows_in_read_at_year_kwh_tier",
        "s.distinct_quality_in_read_at_year_kwh_tier",
        "s.site_zone",
        "s.site_zone_ref",
        "s.quality_len",
        "s.kwh_distance_filled",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

result = (
    line_level
    .groupBy("kwh_tier")
    .pivot("site_zone", ["Z3", "Z2"])
    .agg(F.max("distinct_quality_in_read_at_year_kwh_tier"))
    .orderBy("kwh_tier")
)
