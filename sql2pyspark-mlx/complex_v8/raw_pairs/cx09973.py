from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_full = Window.orderBy(F.col("voltage").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id", "reading_id")
w_quality_by_voltage_running = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_reading_id = Window.partitionBy("quality", "reading_id")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

site_id_stats = (
    cleaned_readings
    .groupBy("site_id")
    .agg(F.max("voltage").alias("max_voltage"))
)

compared = (
    cleaned_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") == F.col("g.max_voltage"))
    .select(
        "s.*",
        "g.max_voltage",
        F.round(F.col("s.voltage") - F.col("g.max_voltage"), 2).alias("voltage_minus_max"),
    )
)

detail_rows = (
    compared.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.max_voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

tagged = (
    detail_rows
    .select(
        "id",
        "voltage",
        "quality",
        "reading_id",
        "reading_kwh",
        "reading_quality",
        F.concat(F.col("reading_quality"), F.lit("#"), F.col("reading_id").cast("string")).alias("reading_quality_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("reading_quality")), " ", "_")).alias("reading_quality_slug"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.nth_value("reading_kwh", 2).over(w_all_by_voltage_full).alias("second_reading_kwh"),
        F.row_number().over(w_quality_by_voltage).alias("seq_no"),
        F.min("reading_kwh").over(w_quality_by_voltage_running).alias("min_reading_kwh_so_far"),
        F.lead("voltage", 1, 0).over(w_quality_by_voltage).alias("next_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_reading_id), F.lit(0)), 4).alias("voltage_share_of_quality_reading_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        "voltage",
        "reading_id",
        "reading_kwh",
        "reading_quality",
        "reading_quality_ref",
        "reading_quality_slug",
        "second_reading_kwh",
        "seq_no",
        "next_voltage",
        F.abs(F.col("reading_kwh") - 57.3).alias("reading_kwh_distance"),
    )
    .orderBy("reading_kwh_distance", F.col("voltage").desc_nulls_first())
)
