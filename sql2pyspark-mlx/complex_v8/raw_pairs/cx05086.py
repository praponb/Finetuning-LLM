from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_full = Window.orderBy("read_at", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_ties = Window.orderBy("read_at")
w_reading_quality_site_id = Window.partitionBy("reading_quality", "site_id")

filtered = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-24").cast("date"))
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

line_level_readings = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isin("EST", "OK")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

metrics = (
    line_level_readings
    .select(
        "*",
        F.nth_value("read_at", 2).over(w_all_by_read_at_full).alias("second_read_at"),
        F.rank().over(w_all_by_read_at_ties).alias("read_at_rank"),
        F.first("reading_read_at").over(w_all_by_read_at_full).alias("first_reading_read_at"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_reading_quality_site_id), F.lit(0)), 4).alias("kwh_share_of_reading_quality_site_id"),
        F.last("reading_read_at").over(w_all_by_read_at_full).alias("last_reading_read_at"),
    )
)

with_details_reading = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.reading_id",
        "s.reading_read_at",
        "s.second_read_at",
        "s.read_at_rank",
        "s.first_reading_read_at",
        "s.kwh_share_of_reading_quality_site_id",
        "s.last_reading_read_at",
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

enriched_reading = (
    with_details_reading.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.id") == F.col("s.reading_id")) & (F.col("m.quality") == "BAD"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.reading_id",
        "s.reading_read_at",
        "s.second_read_at",
        "s.read_at_rank",
        "s.first_reading_read_at",
        "s.kwh_share_of_reading_quality_site_id",
        "s.last_reading_read_at",
        "s.reading_site_id",
        "s.reading_kwh",
        "s.reading_voltage",
        F.col("m.read_at").alias("reading_read_at_2"),
    )
)

result = (
    enriched_reading
    .select(
        "id",
        "site_id",
        "reading_read_at",
        "second_read_at",
        "read_at_rank",
        "first_reading_read_at",
        "kwh_share_of_reading_quality_site_id",
        "last_reading_read_at",
        "reading_kwh",
        "reading_read_at_2",
        F.date_format(F.col("reading_read_at"), "yyyy-MM-dd").alias("reading_read_at_label"),
        (F.col("reading_kwh") + F.col("reading_voltage")).alias("reading_kwh_plus_reading_voltage"),
    )
    .orderBy(F.col("first_reading_read_at").asc())
)
