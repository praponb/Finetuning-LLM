from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.abs(F.col("voltage") - 222.6).alias("voltage_distance"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

with_children_readings = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at_date",
        "s.read_at_label",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

site_id_stats = (
    with_children_readings
    .groupBy("site_id")
    .agg(F.max("reading_kwh").alias("max_reading_kwh"))
)

above_average = (
    with_children_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.reading_kwh") == F.col("g.max_reading_kwh"))
    .select(
        "s.*",
        "g.max_reading_kwh",
        F.round(F.col("s.reading_kwh") - F.col("g.max_reading_kwh"), 2).alias("reading_kwh_minus_max"),
    )
)

screened = (
    above_average
    .crossJoin(above_average.agg(F.min("max_reading_kwh").alias("overall_min_max_reading_kwh")))
    .filter(F.col("max_reading_kwh") > F.col("overall_min_max_reading_kwh"))
    .drop("overall_min_max_reading_kwh")
)

tagged = (
    screened
    .select(
        "id",
        "site_id",
        "read_at_label",
        "reading_id",
        "reading_kwh",
        "max_reading_kwh",
        "reading_kwh_minus_max",
        F.concat(F.col("read_at_label"), F.lit("#"), F.col("reading_id").cast("string")).alias("read_at_label_ref"),
    )
)

result = (
    tagged
    .select(
        "id",
        "reading_id",
        "reading_kwh",
        "max_reading_kwh",
        "reading_kwh_minus_max",
        "read_at_label_ref",
        F.abs(F.col("reading_kwh") - 63.4).alias("reading_kwh_distance"),
        F.round(F.col("max_reading_kwh") * 1.1, 2).alias("max_reading_kwh_adj"),
    )
)
