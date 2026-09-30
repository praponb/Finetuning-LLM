from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("voltage").between(216.8, 235.8)) | (F.col("kwh") >= 43.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_stats = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

with_children_readings = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.last_reading_read_at",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    with_children_readings
    .select(
        "id",
        "reading_id",
        F.col("voltage").alias("voltage_v"),
        F.col("reading_kwh").alias("m_reading_kwh"),
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["voltage_v", "m_reading_kwh"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "reading_id", "measure")
)
