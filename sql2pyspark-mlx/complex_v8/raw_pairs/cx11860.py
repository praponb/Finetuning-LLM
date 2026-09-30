from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_reading_count = Window.partitionBy("quality", "reading_count")
w_read_at_month_reading_count = Window.partitionBy(F.month("read_at"), "reading_count").orderBy("read_at", "id")
w_site_id_by_voltage_distance = Window.partitionBy("site_id").orderBy(F.col("voltage_distance").desc(), "id").rowsBetween(-2, -1)
w_distinct_site_id_in_quality = Window.partitionBy("distinct_site_id_in_quality_reading_count").orderBy(F.col("kwh").desc(), "id")

recent = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.5, 2).alias("kwh_adj"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.count("*").alias("reading_count"),
        F.max("kwh").alias("max_reading_kwh"),
    )
)

with_stats = (
    recent.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.avg_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.max_reading_kwh",
    )
)

with_windows = (
    with_stats
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "avg_reading_voltage",
        "max_reading_kwh",
        F.size(F.collect_set("site_id").over(w_quality_reading_count)).alias("distinct_site_id_in_quality_reading_count"),
        (F.col("voltage") - F.lag("voltage").over(w_read_at_month_reading_count)).alias("voltage_change"),
    )
)

derived = with_windows.select("*", F.abs(F.col("voltage") - 220.7).alias("voltage_distance"))

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.sum("kwh").alias("total_reading_kwh"))
)

summarized_rows = (
    derived.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_kwh")
)

windowed = (
    summarized_rows
    .select(
        "*",
        F.round(F.avg("voltage_change").over(w_site_id_by_voltage_distance), 2).alias("voltage_change_avg_2_prior"),
        F.lead("max_reading_kwh", 2).over(w_distinct_site_id_in_quality).alias("next2_max_reading_kwh"),
    )
)

derived_2 = (
    windowed
    .select(
        "*",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
    )
)

result = (
    derived_2
    .groupBy("read_at_month", "distinct_site_id_in_quality_reading_count")
    .pivot("quality", ["EST", "BAD"])
    .agg(F.max("voltage_change").alias("max_v"), F.count("voltage_change").alias("n"))
)
