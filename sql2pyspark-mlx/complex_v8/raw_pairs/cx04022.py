from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.col("quality") == "EST") & (F.col("voltage").between(216.1, 243.1)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") <= 233.1) & (F.col("m.kwh") <= 109.3), "left_anti")
    .filter(F.col("site_id").isNotNull())
)

annotated = (
    with_activity
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

without_matches = (
    annotated
    .crossJoin(annotated.agg(F.avg("least_kwh_voltage").alias("overall_avg_least_kwh_voltage")))
    .filter(F.col("least_kwh_voltage") > F.col("overall_avg_least_kwh_voltage"))
    .drop("overall_avg_least_kwh_voltage")
)

line_level_readings = (
    without_matches.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality_or_unknown",
        "s.least_kwh_voltage",
        "s.read_at_label",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

result = (
    line_level_readings
    .groupBy("read_at")
    .pivot("reading_quality", ["EST", "OK"])
    .agg(F.sum("reading_voltage").alias("amount"), F.count("reading_voltage").alias("n"))
    .orderBy("read_at")
)
