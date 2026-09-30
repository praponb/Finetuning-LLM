from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 116.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("kwh") >= 89.8, "A").when(F.col("kwh") >= 34.1, "B").otherwise("C").alias("kwh_tier"),
    )
)

with_children = (
    prepared_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(1, 3)), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.kwh_tier",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    with_children
    .select(
        "id",
        "site_id",
        "kwh",
        "reading_id",
        "reading_quality",
        "reading_read_at",
    )
    .orderBy("id", F.col("kwh").asc_nulls_last(), "reading_id")
    .limit(20)
)
