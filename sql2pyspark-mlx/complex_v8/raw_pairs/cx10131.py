from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter(F.col("voltage").between(221.8, 235.8))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

with_parent = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select("s.*", "si.site_name")
)

with_children = (
    with_parent.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

qualified = (
    with_children.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") > 219.5) & (F.col("m.kwh") > 26.9), "left_anti")
)

scored = (
    qualified
    .select(
        "*",
        F.greatest(F.col("reading_kwh"), F.col("reading_voltage")).alias("greatest_reading_kwh_reading_voltage"),
    )
)

result = (
    scored
    .select(
        "id",
        "reading_id",
        "voltage",
        "reading_voltage",
        "reading_kwh",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["voltage", "reading_voltage", "reading_kwh"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
