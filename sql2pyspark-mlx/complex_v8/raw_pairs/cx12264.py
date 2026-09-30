from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("kwh") >= 30.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
    )
)

detail_rows_readings = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.voltage_adj",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    detail_rows_readings
    .groupBy("reading_id", "site_id")
    .pivot("quality", ["BAD", "OK"])
    .agg(F.max("kwh"))
)
