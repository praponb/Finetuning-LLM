from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reading_read_at = Window.orderBy("reading_read_at", "id", "reading_id", "reading_id_2")
w_all_by_reading_read_at_full = Window.orderBy("reading_read_at", "id", "reading_id", "reading_id_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reading_quality = Window.partitionBy("reading_quality")
w_all_by_reading_read_at_ties = Window.orderBy("reading_read_at")

valid = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-12 12:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

derived = (
    valid
    .select(
        "id",
        "site_id",
        "voltage",
        "quality_or_unknown",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

with_children = (
    derived.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.least_kwh_voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

detail_rows_readings = (
    with_children.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(1, 3)), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.least_kwh_voltage",
        "s.reading_id",
        "s.reading_quality",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage_2"),
    )
)

windowed = (
    detail_rows_readings
    .select(
        "*",
        F.lag("reading_voltage_2").over(w_all_by_reading_read_at).alias("prev_reading_voltage_2"),
        F.first("reading_read_at").over(w_all_by_reading_read_at_full).alias("first_reading_read_at"),
        F.round(F.col("reading_voltage_2") - F.avg("reading_voltage_2").over(w_reading_quality), 2).alias("reading_voltage_2_vs_reading_quality_avg"),
        F.row_number().over(w_all_by_reading_read_at).alias("reading_read_at_seq"),
        F.round(F.percent_rank().over(w_all_by_reading_read_at_ties), 3).alias("reading_read_at_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "least_kwh_voltage",
        "reading_id",
        "reading_quality",
        "reading_id_2",
        "first_reading_read_at",
        "reading_voltage_2_vs_reading_quality_avg",
    )
    .orderBy(F.col("reading_id").desc())
)
