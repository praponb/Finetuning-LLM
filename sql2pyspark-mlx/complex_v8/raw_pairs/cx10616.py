from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("voltage") >= 220.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

tagged = (
    prepared_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_slug",
        "voltage_bucket",
        F.upper(F.trim(F.col("quality_slug"))).alias("quality_slug_clean"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
        F.greatest(F.col("voltage_bucket"), F.col("greatest_voltage_kwh")).alias("greatest_voltage_bucket_greatest_voltage_kwh"),
    )
)

with_details = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.quality_slug",
        "s.quality_slug_clean",
        "s.read_at_dayofweek",
        "s.greatest_voltage_bucket_greatest_voltage_kwh",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

enriched = (
    with_details
    .filter(F.col("quality").isNotNull())
    .select("*", F.year(F.col("read_at")).alias("read_at_year"))
)

result = (
    enriched
    .select("id", "greatest_voltage_bucket_greatest_voltage_kwh", "site_installed_kw")
    .unpivot(
        ids=["id"],
        values=["greatest_voltage_bucket_greatest_voltage_kwh", "site_installed_kw"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
