from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_id = Window.partitionBy("reading_id")
w_reading_id_by_reading_read_a = Window.partitionBy("reading_id").orderBy(F.col("reading_read_at").desc(), "id", "reading_id")

filtered = (
    df_meter_readings
    .filter(F.col("kwh") >= 14.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

exploded_detail = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(1, 2)), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

windowed = (
    exploded_detail
    .select(
        "*",
        F.sum("kwh").over(w_reading_id).alias("reading_id_sum_kwh"),
        F.round(F.percent_rank().over(w_reading_id_by_reading_read_a), 3).alias("reading_read_at_percent_rank"),
    )
)

with_details = (
    windowed.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") < F.lit("2025-05-10").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_read_at",
        "s.reading_kwh",
        "s.reading_id_sum_kwh",
        "s.reading_read_at_percent_rank",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

enriched = (
    with_details
    .select(
        "*",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.date_trunc("year", F.col("site_commissioned_on")).alias("site_commissioned_on_year"),
    )
)

line_level = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.reading_id",
        "s.reading_read_at_percent_rank",
        "s.site_zone",
        "s.kwh_filled",
        "s.site_commissioned_on_year",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.kwh").alias("reading_kwh_2"),
    )
)

result = (
    line_level
    .select(
        "id",
        "site_id",
        "reading_read_at_percent_rank",
        "site_zone",
        "kwh_filled",
        "site_commissioned_on_year",
        "reading_id_2",
        "reading_quality",
        "reading_read_at_2",
    )
    .orderBy("site_zone")
)
