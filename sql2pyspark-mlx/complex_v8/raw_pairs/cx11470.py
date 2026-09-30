from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_dayofweek_site_id = Window.partitionBy("read_at_dayofweek", "site_id")
w_site_id_by_kwh_prior_2 = Window.partitionBy("site_id").orderBy(F.col("kwh").asc_nulls_last(), "id", "reading_id").rowsBetween(-2, -1)
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy(F.col("kwh").asc_nulls_last(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_meter_readings
    .filter((F.col("voltage") < 241.8) & (F.col("site_id") >= 4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (~F.col("m.quality").isin("EST", "BAD")) & (F.to_date(F.col("m.read_at")) < F.lit("2025-02-18").cast("date")), "left_semi")
)

line_level_readings = (
    without_matches.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.read_at_dayofweek",
        "s.quality_group",
        "s.voltage_filled",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

ranked = (
    line_level_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "read_at_dayofweek",
        "quality_group",
        "voltage_filled",
        "reading_id",
        "reading_voltage",
        "reading_read_at",
        "reading_kwh",
        F.count(F.lit(1)).over(w_read_at_dayofweek_site_id).alias("rows_in_read_at_dayofweek_site_id"),
        F.sum("read_at_dayofweek").over(w_site_id_by_kwh_prior_2).alias("read_at_dayofweek_sum_2_prior"),
        F.min("reading_kwh").over(w_read_at_dayofweek_site_id).alias("read_at_dayofweek_site_id_min_reading_kwh"),
        F.size(F.collect_set("quality_group").over(w_read_at_dayofweek_site_id)).alias("distinct_quality_group_in_read_at_dayofweek_site"),
        F.nth_value("kwh", 2).over(w_site_id_by_kwh_full).alias("second_kwh"),
    )
)

result = (
    ranked
    .rollup("distinct_quality_group_in_read_at_dayofweek_site", "quality_group", "read_at_dayofweek")
    .agg(F.countDistinct("site_id").alias("distinct_site_ids"))
)
