from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("quality").isin("OK", "BAD")) & (F.col("kwh") <= 111.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") < 373.3), "left_semi")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

profiled = (
    with_activity.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        "m.last_reading_read_at",
        "m.first_reading_read_at",
    )
)

result = (
    profiled
    .select(
        "id",
        "max_reading_voltage",
        "kwh",
        "voltage",
    )
    .unpivot(
        ids=["id"],
        values=["max_reading_voltage", "kwh", "voltage"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
