from pyspark.sql import functions as F

prepared_sites = df_sites.filter(F.col("tariff") == "TOU")

scored = (
    prepared_sites
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        F.quarter(F.col("commissioned_on")).alias("commissioned_on_quarter"),
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
)

with_children = (
    scored.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.site_name",
        "s.commissioned_on_quarter",
        "s.site_name_len",
        "s.zone_or_unknown",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    with_children
    .select(
        "id",
        "reading_id",
        F.col("site_name_len").alias("site_name_len_v"),
        F.col("commissioned_on_quarter").alias("commissioned_on_quarter_v"),
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["site_name_len_v", "commissioned_on_quarter_v"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .orderBy("id", "reading_id", "measure")
)
