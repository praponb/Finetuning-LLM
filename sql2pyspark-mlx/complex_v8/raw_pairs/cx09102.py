from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_ties = Window.orderBy("kwh")
w_quality_by_kwh = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_quality_quality_group = Window.partitionBy("quality", "quality_group")

recent_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 241.8) | (F.to_date(F.col("read_at")) < F.lit("2025-02-18").cast("date")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

windowed = (
    recent_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_date",
        F.round(F.cume_dist().over(w_all_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.lag("voltage", 3, 0).over(w_quality_by_kwh).alias("prev3_voltage"),
        F.count(F.lit(1)).over(w_quality_quality_group).alias("rows_in_quality_quality_group"),
        F.max("voltage").over(w_quality_quality_group).alias("quality_quality_group_max_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"))
)

with_counts = (
    windowed.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_kwh")
)

qualified = (
    with_counts.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

without_matches = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") >= 34.6), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "read_at",
        "voltage",
        "read_at_date",
        "prev3_voltage",
        "rows_in_quality_quality_group",
        "quality_quality_group_max_voltage",
        "avg_reading_kwh",
        F.least(F.col("kwh_cume_dist"), F.col("voltage")).alias("least_kwh_cume_dist_voltage"),
    )
    .orderBy(F.col("quality_quality_group_max_voltage").desc(), "avg_reading_kwh")
)
