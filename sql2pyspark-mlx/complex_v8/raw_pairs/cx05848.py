from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("voltage") <= 240.2) & (F.col("kwh").between(35.0, 116.8)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

annotated = (
    scoped
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
    )
)

with_stats = (
    annotated.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.avg_reading_voltage",
    )
)

result = (
    with_stats
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "read_at_year",
        "quality_slug",
        "reading_count",
        "avg_reading_voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality_slug")), " ", "_")).alias("quality_slug_slug"),
    )
    .orderBy(F.col("voltage").desc(), F.col("read_at_year").desc())
)
