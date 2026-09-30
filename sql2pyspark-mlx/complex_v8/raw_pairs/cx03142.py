from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter((F.col("kwh") > 35.9) & (F.col("site_id") <= 5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_children_readings = (
    valid.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.days_since_read_at",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

with_children = (
    with_children_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") >= 26.9), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.reading_id",
        "s.reading_read_at",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality_2"),
        F.col("m.voltage").alias("reading_voltage_2"),
    )
)

filtered = (
    with_children
    .crossJoin(with_children.agg(F.min("reading_voltage_2").alias("overall_min_reading_voltage_2")))
    .filter(F.col("reading_voltage_2") > F.col("overall_min_reading_voltage_2"))
    .drop("overall_min_reading_voltage_2")
)

result = (
    filtered
    .rollup("reading_id_2", "reading_quality_2")
    .agg(
        F.grouping_id().alias("gid"),
        F.min("reading_kwh").alias("min_reading_kwh"),
        F.countDistinct("reading_id_2").alias("distinct_reading_id_2s"),
    )
    .select(
        "reading_id_2",
        F.coalesce(F.col("reading_quality_2"), F.lit("ALL")).alias("reading_quality_2"),
        "gid",
        "min_reading_kwh",
        "distinct_reading_id_2s",
    )
    .orderBy(
        F.col("reading_id_2").asc_nulls_last(),
        F.col("reading_quality_2").asc_nulls_last(),
    )
)
