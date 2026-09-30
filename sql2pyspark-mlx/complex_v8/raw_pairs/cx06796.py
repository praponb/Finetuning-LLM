from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

annotated = (
    recent_readings
    .select(
        "*",
        F.length(F.trim(F.col("quality_slug"))).alias("quality_slug_len"),
        F.length(F.trim(F.col("quality_slug"))).alias("quality_slug_len_2"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

without_matches = (
    annotated.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    without_matches
    .filter(F.col("quality_slug_len") <= 12)
    .select(
        "id",
        "site_id",
        "kwh",
        "quality_slug",
        "quality_slug_len",
    )
    .orderBy(F.col("id").desc())
)
