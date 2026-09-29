from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.col("voltage") < 233.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
    )
)

with_parent_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_month",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

annotated = (
    with_parent_site
    .select(
        "id",
        "kwh",
        "voltage",
        "read_at_month",
        "site_tariff",
        "site_installed_kw",
        F.concat(F.col("site_tariff"), F.lit("#"), F.col("id").cast("string")).alias("site_tariff_ref"),
        F.when(F.col("kwh") >= 78.6, "HIGH").when(F.col("kwh") >= 37.0, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

result = (
    annotated
    .select(
        "id",
        F.col("site_installed_kw").alias("site_installed_kw_v"),
        F.col("voltage").alias("voltage_v"),
    )
    .unpivot(
        ids=["id"],
        values=["site_installed_kw_v", "voltage_v"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
