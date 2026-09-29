from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

derived = (
    valid
    .select(
        "*",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

annotated = (
    derived
    .filter(F.col("kwh").between(29.2, 106.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "read_at_date",
        "kwh_per_voltage",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.min("kwh").alias("min_reading_kwh"),
        F.max("read_at").alias("last_reading_read_at_2"),
    )
)

profiled = (
    annotated.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        "m.min_reading_kwh",
        "m.last_reading_read_at_2",
    )
)

without_matches = (
    profiled
    .crossJoin(profiled.agg(F.avg("kwh_per_voltage").alias("overall_avg_kwh_per_voltage")))
    .filter(F.col("kwh_per_voltage") > F.col("overall_avg_kwh_per_voltage"))
    .drop("overall_avg_kwh_per_voltage")
)

joined = (
    without_matches.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_date",
        "s.kwh_per_voltage",
        "s.read_at_dayofweek",
        "s.min_reading_kwh",
        "s.last_reading_read_at_2",
        "si.site_name",
    )
)

result = (
    joined
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "read_at_date",
        "kwh_per_voltage",
        "read_at_dayofweek",
        "site_name",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at_date")).alias("days_since_read_at_date"),
    )
    .orderBy(F.col("site_id").desc(), F.col("kwh").asc_nulls_last())
)
