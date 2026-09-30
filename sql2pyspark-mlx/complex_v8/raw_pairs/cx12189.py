from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_read_at_year_by_volt = Window.partitionBy(F.year("reading_read_at")).orderBy(F.col("voltage").desc())
w_reading_read_at_year_by_volt_2 = Window.partitionBy(F.year("reading_read_at")).orderBy(F.col("voltage").desc(), "id", "reading_id")
w_reading_id_reading_site_id_b = Window.partitionBy("reading_id", "reading_site_id").orderBy("reading_kwh", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_reading_id = Window.partitionBy("quality", "reading_id")

cleaned = (
    df_meter_readings
    .filter(F.col("site_id") >= 6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") != "BAD"), "left_semi")
)

with_details = (
    qualified.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_plus_voltage",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

line_level = (
    with_details.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(2, 3)), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.kwh_plus_voltage",
        "s.site_zone",
        "s.site_name",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

enriched_reading = (
    line_level.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.site_zone",
        "s.site_name",
        "s.reading_id",
        "s.reading_read_at",
        "s.reading_voltage",
        "s.reading_quality",
        "s.reading_kwh",
        F.col("m.site_id").alias("reading_site_id"),
    )
)

qualified_2 = (
    enriched_reading.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.reading_site_id"), "left_semi")
    .filter(F.col("reading_id").isNotNull())
)

windowed = (
    qualified_2
    .select(
        "*",
        F.round(F.cume_dist().over(w_reading_read_at_year_by_volt), 3).alias("voltage_cume_dist"),
        F.dense_rank().over(w_reading_read_at_year_by_volt_2).alias("voltage_dense_rank"),
        F.last("reading_read_at").over(w_reading_id_reading_site_id_b).alias("last_reading_read_at"),
        F.round(F.col("reading_voltage") / F.nullif(F.sum("reading_voltage").over(w_quality_reading_id), F.lit(0)), 4).alias("reading_voltage_share_of_quality_reading_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "site_zone",
        "site_name",
        "reading_id",
        "reading_voltage",
        "reading_quality",
        "reading_kwh",
        "reading_site_id",
        "voltage_dense_rank",
        "last_reading_read_at",
        "reading_voltage_share_of_quality_reading_id",
    )
    .orderBy(F.col("reading_site_id").desc())
)
