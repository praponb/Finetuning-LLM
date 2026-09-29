from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_read_at_dayofweek = Window.orderBy("read_at_dayofweek", "id")
w_site_id_by_total_reading_kwh = Window.partitionBy("site_id").orderBy("total_reading_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_read_at_dayofweek = Window.partitionBy("quality", "read_at_dayofweek")

recent_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 2)) | (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.sum("kwh").alias("total_reading_kwh"))
)

summarized_rows = (
    recent_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_kwh")
)

windowed = (
    summarized_rows
    .select(
        "*",
        F.min("kwh").over(w_site_id).alias("site_id_min_kwh"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_read_at_dayofweek)).alias("kwh_change"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.round(F.col("total_reading_kwh") / F.nullif(F.sum("total_reading_kwh").over(w_site_id), F.lit(0)), 4).alias("total_reading_kwh_share_of_site_id"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "site_id_min_kwh",
        "kwh_change",
        "kwh_vs_site_id_avg",
        "total_reading_kwh_share_of_site_id",
        "distinct_quality_in_site_id",
        F.nth_value("kwh_change", 2).over(w_site_id_by_total_reading_kwh).alias("second_kwh_change"),
        F.count(F.lit(1)).over(w_quality_read_at_dayofweek).alias("rows_in_quality_read_at_dayofweek"),
        F.first("read_at_dayofweek").over(w_site_id_by_total_reading_kwh).alias("first_read_at_dayofweek"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") > 216.8), "left_anti")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
    )
)

with_stats = (
    qualified.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.avg_reading_kwh",
    )
)

result = (
    with_stats
    .filter(F.col("site_id") >= 5)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "site_id_min_kwh",
        "kwh_change",
        "kwh_vs_site_id_avg",
        "total_reading_kwh_share_of_site_id",
        "rows_in_quality_read_at_dayofweek",
        "first_read_at_dayofweek",
        "reading_count",
        "avg_reading_kwh",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
    .orderBy(
        F.col("first_read_at_dayofweek").desc(),
        F.col("kwh").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
