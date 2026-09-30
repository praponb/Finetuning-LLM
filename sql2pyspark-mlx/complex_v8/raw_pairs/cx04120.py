from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_by_voltage = Window.partitionBy(F.to_date("read_at")).orderBy(F.col("voltage").asc_nulls_last(), "id")
w_site_id_by_days_since_read_a = Window.partitionBy("site_id").orderBy(F.col("days_since_read_at").desc(), "id")
w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))

filtered_readings = (
    df_meter_readings
    .filter((F.col("voltage") > 218.2) & (F.to_date(F.col("read_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_stats = (
    filtered_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

with_activity = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") > 28.4) & (F.col("m.voltage").between(215.8, 235.8)), "left_anti")
)

with_trends = (
    with_activity
    .select(
        "*",
        F.lead("kwh", 2).over(w_read_at_to_date_by_voltage).alias("next2_kwh"),
        F.ntile(5).over(w_site_id_by_days_since_read_a).alias("days_since_read_at_quintile"),
        F.max("reading_count").over(w_site_id_read_at_to_date).alias("site_id_read_at_to_date_max_reading_count"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(~F.col("si.zone").isin("Z3", "Z4"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.days_since_read_at",
        "s.read_at_day",
        "s.next2_kwh",
        "s.days_since_read_at_quintile",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(~F.col("quality").isin("BAD", "EST"), 1)).alias("reading_quality_count"),
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
    )
)

summarized_rows = (
    joined.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.avg_reading_voltage",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "days_since_read_at",
        "read_at_day",
        "days_since_read_at_quintile",
        "site_commissioned_on",
        "site_zone",
        "reading_count",
        "reading_quality_count",
        "avg_reading_voltage",
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
    )
    .orderBy("read_at", "days_since_read_at")
)
