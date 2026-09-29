from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 113.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

detail_rows_readings = (
    scoped_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality_group",
        "s.read_at_label",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    detail_rows_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "quality_group",
        "read_at_label",
        "reading_quality",
        "reading_kwh",
        "reading_read_at",
        F.coalesce(F.col("reading_kwh"), F.lit(0.0)).alias("reading_kwh_filled"),
    )
    .orderBy("read_at", "reading_kwh_filled")
)
