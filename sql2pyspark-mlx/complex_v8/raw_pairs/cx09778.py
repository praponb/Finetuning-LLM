from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_kwh_moving_1 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_kwh_full = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) | (F.col("quality").isin("EST", "OK")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

profiled = (
    scoped_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

windowed = (
    profiled
    .select(
        "*",
        F.round(F.avg("voltage").over(w_all_by_kwh_moving_1), 2).alias("voltage_avg_2"),
        F.nth_value("kwh", 2).over(w_all_by_kwh_full).alias("second_kwh"),
    )
)

stats = (
    windowed
    .filter(F.col("voltage_avg_2") > 217.0)
    .groupBy(
        "site_id",
        "quality",
        F.year("first_reading_read_at").alias("first_reading_read_at_year"),
    )
    .agg(
        F.min("first_reading_read_at").alias("first_first_reading_read_at"),
        F.count("*").alias("n_rows"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
)

with_details_site = (
    stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.quality",
        "s.first_reading_read_at_year",
        "s.first_first_reading_read_at",
        "s.distinct_site_ids",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    with_details_site
    .select(
        "site_id",
        "quality",
        "first_reading_read_at_year",
        "first_first_reading_read_at",
        "site_tariff",
        "site_zone",
        "site_commissioned_on",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("site_commissioned_on")).alias("days_since_site_commissioned_on"),
    )
    .orderBy(F.col("first_reading_read_at_year").asc())
)
