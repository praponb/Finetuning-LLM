from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.kwh_per_voltage",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_parent
    .select("id", "kwh_per_voltage", "voltage")
    .unpivot(
        ids=["id"],
        values=["kwh_per_voltage", "voltage"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
