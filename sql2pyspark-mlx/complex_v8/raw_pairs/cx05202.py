from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality_count_read_a = Window.partitionBy("reading_quality_count", "read_at_month").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reading_quality_count_read_a_2 = Window.partitionBy("reading_quality_count", "read_at_month").orderBy(F.col("read_at").desc(), "id")
w_reading_quality_count_site_i = Window.partitionBy("reading_quality_count", "site_id")
w_read_at_month_by_read_at_mon = Window.partitionBy("read_at_month").orderBy("read_at_month")

scoped_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.month(F.col("read_at")).alias("read_at_month"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
    )
)

summarized_rows = (
    scoped_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.avg_reading_voltage",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

sequenced = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "read_at_month",
        "avg_reading_voltage",
        F.last("avg_reading_voltage").over(w_reading_quality_count_read_a).alias("last_avg_reading_voltage"),
        F.dense_rank().over(w_reading_quality_count_read_a_2).alias("read_at_dense_rank"),
        F.min("reading_quality_count").over(w_reading_quality_count_site_i).alias("reading_quality_count_site_id_min_reading_qualit"),
        F.dense_rank().over(w_read_at_month_by_read_at_mon).alias("read_at_month_dense_rank"),
    )
)

with_parent = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.commissioned_on").alias("site_commissioned_on"))
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

profiled = (
    with_parent.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

tagged = (
    profiled
    .select(
        "id",
        "quality",
        "last_avg_reading_voltage",
        "reading_quality_count_site_id_min_reading_qualit",
        "site_commissioned_on",
        "reading_count",
        F.coalesce(F.col("avg_reading_voltage"), F.lit(0.0)).alias("avg_reading_voltage_filled"),
    )
)

without_matches = (
    tagged
    .crossJoin(tagged.agg(F.max("avg_reading_voltage_filled").alias("overall_max_avg_reading_voltage_filled")))
    .filter(F.col("avg_reading_voltage_filled") < F.col("overall_max_avg_reading_voltage_filled"))
    .drop("overall_max_avg_reading_voltage_filled")
)

result = (
    without_matches
    .select(
        "id",
        "quality",
        "site_commissioned_on",
        "reading_count",
        "avg_reading_voltage_filled",
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.coalesce(F.col("last_avg_reading_voltage"), F.lit(0.0)).alias("last_avg_reading_voltage_filled"),
    )
    .orderBy(
        F.col("avg_reading_voltage_filled").desc(),
        F.col("last_avg_reading_voltage_filled").desc_nulls_first(),
    )
)
