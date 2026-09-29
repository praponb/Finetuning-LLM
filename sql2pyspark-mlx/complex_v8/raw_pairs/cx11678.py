from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter((F.col("kwh") >= 42.1) & (F.col("voltage") >= 216.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("voltage") * 1.1, 2).alias("voltage_adj"),
    )
)

detail_rows = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.voltage_adj",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    detail_rows
    .rollup("reading_id", "quality")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("quality").isNotNull(), 1).otherwise(0)).alias("n_quality_is_not_null"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("reading_id").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
