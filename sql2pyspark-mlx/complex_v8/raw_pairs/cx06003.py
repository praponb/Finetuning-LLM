from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.col("voltage") >= 220.7) & (F.col("kwh") < 109.3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

joined_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

annotated = (
    joined_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "site_installed_kw",
        "site_zone",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
    )
)

result = (
    annotated
    .filter(F.col("kwh") <= 109.0)
    .select(
        "id",
        "read_at",
        "kwh",
        "site_installed_kw",
    )
    .orderBy(F.col("id").asc())
)
