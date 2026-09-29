from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter(F.col("kwh") >= 45.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.when(F.col("voltage") >= 229.1, "A").when(F.col("voltage") >= 218.4, "B").otherwise("C").alias("voltage_tier"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.voltage_tier",
        "s.quality_ref",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_parent
    .cube("site_id", "voltage_tier")
    .agg(
        F.round(F.sum("site_installed_kw") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("site_installed_kw_to_voltage_ratio"),
        F.count("*").alias("n_rows"),
        F.max("voltage").alias("max_voltage"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("voltage_tier"), F.lit("ALL")).alias("voltage_tier"),
        "site_installed_kw_to_voltage_ratio",
        "n_rows",
        "max_voltage",
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("voltage_tier").asc_nulls_last(),
    )
)
