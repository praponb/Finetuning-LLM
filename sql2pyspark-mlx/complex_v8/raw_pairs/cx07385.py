from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("kwh") <= 106.0) | (F.col("site_id") <= 8))
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
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (~F.col("m.quality").isin("EST", "BAD")), "left_semi")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 218.5), "left_anti")
    .filter(F.col("site_id") >= 6)
)

tagged = (
    with_activity_2
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug_2"),
    )
)

result = (
    tagged
    .select("id", "voltage", "kwh")
    .unpivot(
        ids=["id"],
        values=["voltage", "kwh"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
